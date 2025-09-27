#!/usr/bin/env python3
"""
Comprehensive Anti-Detection Middleware for NEQ Scraper

This middleware combines:
- IP rotation with proxy sleep mechanism
- User agent rotation per proxy
- Session management with cookies
- Intelligent proxy health monitoring
"""

from scrapy import signals
import random
import time
import json
import base64
from pathlib import Path
from collections import defaultdict, deque
from urllib.parse import urlparse


class ComprehensiveAntiDetectionMiddleware:
    """Comprehensive middleware combining IP rotation, UA rotation, session management, and proxy sleep"""
    
    def __init__(self):
        self.user_agents = [
            # Chrome on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            
            # Firefox on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
            
            # Chrome on macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            
            # Safari on macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
            
            # Edge on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
        ]
        
        # Proxy management
        self.proxy_stats = defaultdict(lambda: {
            'requests': 0,
            'errors': 0,
            'last_used': 0,
            'sleep_until': 0,  # When proxy can be used again
            'consecutive_errors': 0,
            'total_sleep_time': 0,
            'session_cookies': {},  # Store cookies per proxy
            'current_user_agent': None,
            'last_success': 0,
            'blocked_count': 0,
            'success_rate': 0.0
        })
        
        self.proxy_list = []
        self.current_proxy_index = 0
        self.load_proxies()
        
        # Request tracking
        self.request_count = 0
        self.last_proxy_change = 0
        self.proxy_rotation_count = 0
        
    def load_proxies(self):
        """Load proxies from proxies.json"""
        try:
            proxies_path = Path.cwd() / "proxies.json"
            if proxies_path.is_file():
                with proxies_path.open("r", encoding="utf-8-sig") as pf:
                    loaded = json.load(pf)
                    if isinstance(loaded, list):
                        self.proxy_list = [str(x).strip() for x in loaded if str(x).strip()]
                        print(f"Loaded {len(self.proxy_list)} proxies for comprehensive anti-detection")
        except Exception as e:
            print(f"Failed to load proxies: {e}")
    
    def get_available_proxy(self):
        """Get next available proxy (not in sleep mode)"""
        if not self.proxy_list:
            return None
        
        current_time = time.time()
        available_proxies = []
        
        for i, proxy in enumerate(self.proxy_list):
            stats = self.proxy_stats[proxy]
            # Skip if proxy is sleeping
            if stats['sleep_until'] > current_time:
                continue
            # Skip if too many consecutive errors
            if stats['consecutive_errors'] >= 3:
                continue
            available_proxies.append(i)
        
        if not available_proxies:
            # If no proxies available, reset all and use the first one
            print("All proxies are sleeping, resetting proxy states")
            for proxy in self.proxy_stats:
                self.proxy_stats[proxy]['consecutive_errors'] = 0
                self.proxy_stats[proxy]['sleep_until'] = 0
            available_proxies = [0]
        
        # Use round-robin with randomization
        if available_proxies:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            while self.current_proxy_index not in available_proxies:
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        
        return self.proxy_list[self.current_proxy_index] if self.proxy_list else None
    
    def get_user_agent_for_proxy(self, proxy):
        """Get or assign a user agent for this proxy"""
        stats = self.proxy_stats[proxy]
        
        if not stats['current_user_agent']:
            # Assign a new user agent to this proxy
            stats['current_user_agent'] = random.choice(self.user_agents)
        
        return stats['current_user_agent']
    
    def get_session_cookies_for_proxy(self, proxy):
        """Get session cookies for this proxy"""
        return self.proxy_stats[proxy]['session_cookies']
    
    def set_session_cookies_for_proxy(self, proxy, cookies):
        """Set session cookies for this proxy"""
        self.proxy_stats[proxy]['session_cookies'].update(cookies)
    
    def put_proxy_to_sleep(self, proxy, sleep_minutes=10):
        """Put proxy to sleep for specified minutes"""
        current_time = time.time()
        sleep_seconds = sleep_minutes * 60
        self.proxy_stats[proxy]['sleep_until'] = current_time + sleep_seconds
        self.proxy_stats[proxy]['total_sleep_time'] += sleep_seconds
        self.proxy_stats[proxy]['blocked_count'] += 1
        
        print(f"Proxy {proxy} put to sleep for {sleep_minutes} minutes due to failures")
    
    def rotate_to_next_proxy(self):
        """Force rotation to next proxy"""
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        self.proxy_rotation_count += 1
    
    def process_request(self, request, spider):
        """Process each request with comprehensive anti-detection"""
        current_time = time.time()
        
        # Get available proxy
        proxy = self.get_available_proxy()
        if not proxy:
            spider.logger.error("No available proxies!")
            return None
        
        # Update proxy stats
        stats = self.proxy_stats[proxy]
        stats['requests'] += 1
        stats['last_used'] = current_time
        
        # Parse proxy credentials
        proxy_parts = proxy.split(":")
        if len(proxy_parts) == 4:
            # Format: ip:port:username:password
            ip, port, username, password = proxy_parts
            proxy_url = f"http://{username}:{password}@{ip}:{port}"
            
            # Note: Authentication is included in the proxy URL itself
            # This is the correct format for Scrapy proxy authentication
        else:
            # Format: ip:port (no auth)
            proxy_url = f"http://{proxy}"
        
        # Set proxy
        request.meta['proxy'] = proxy_url
        request.meta['current_proxy'] = proxy
        
        # Get user agent for this proxy
        user_agent = self.get_user_agent_for_proxy(proxy)
        request.headers['User-Agent'] = user_agent
        
        # Get session cookies for this proxy
        session_cookies = self.get_session_cookies_for_proxy(proxy)
        if session_cookies:
            request.cookies.update(session_cookies)
        
        # Add additional headers to mimic real browser
        request.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Track request count and proxy changes
        self.request_count += 1
        if self.last_proxy_change != self.current_proxy_index:
            self.last_proxy_change = self.current_proxy_index
            spider.logger.info(f"Switched to proxy {proxy} (request #{self.request_count})")
        
        spider.logger.debug(f"Request #{self.request_count} using proxy: {proxy}")
        
        return None
    
    def process_response(self, request, response, spider):
        """Process response and manage proxy health"""
        proxy = request.meta.get('current_proxy')
        if not proxy:
            return response
        
        stats = self.proxy_stats[proxy]
        
        # Update session cookies from response
        if response.headers.get('Set-Cookie'):
            cookies = {}
            for cookie in response.headers.getlist('Set-Cookie'):
                # Parse cookie (simplified)
                cookie_parts = cookie.split(';')[0].split('=', 1)
                if len(cookie_parts) == 2:
                    cookies[cookie_parts[0].strip()] = cookie_parts[1].strip()
            self.set_session_cookies_for_proxy(proxy, cookies)
        
        # Check for blocking/failure indicators
        if response.status in [403, 429, 503, 502, 504]:
            stats['consecutive_errors'] += 1
            stats['errors'] += 1
            
            # Put proxy to sleep for 10 minutes on failure
            self.put_proxy_to_sleep(proxy, 10)
            
            spider.logger.warning(f"Proxy {proxy} failed with status {response.status}, sleeping for 10 minutes")
            
            # Force proxy rotation for next request
            self.rotate_to_next_proxy()
            
        else:
            # Reset consecutive errors on successful response
            if stats['consecutive_errors'] > 0:
                spider.logger.info(f"Proxy {proxy} recovered after {stats['consecutive_errors']} errors")
            stats['consecutive_errors'] = 0
            stats['last_success'] = time.time()
            
            # Update success rate
            if stats['requests'] > 0:
                stats['success_rate'] = (stats['requests'] - stats['errors']) / stats['requests']
        
        return response
    
    def process_exception(self, request, exception, spider):
        """Handle exceptions and manage proxy health"""
        proxy = request.meta.get('current_proxy')
        if not proxy:
            return None
        
        stats = self.proxy_stats[proxy]
        stats['errors'] += 1
        stats['consecutive_errors'] += 1
        
        # Put proxy to sleep for 10 minutes on exception
        self.put_proxy_to_sleep(proxy, 10)
        
        spider.logger.error(f"Proxy {proxy} exception: {exception}, sleeping for 10 minutes")
        
        # Force proxy rotation
        self.rotate_to_next_proxy()
        
        return None
    
    def get_proxy_statistics(self):
        """Get comprehensive proxy statistics"""
        stats_summary = {}
        for proxy, stats in self.proxy_stats.items():
            if stats['requests'] > 0:
                stats_summary[proxy] = {
                    'requests': stats['requests'],
                    'errors': stats['errors'],
                    'success_rate': stats['success_rate'],
                    'blocked_count': stats['blocked_count'],
                    'total_sleep_time': stats['total_sleep_time'],
                    'consecutive_errors': stats['consecutive_errors'],
                    'is_sleeping': stats['sleep_until'] > time.time()
                }
        return stats_summary
