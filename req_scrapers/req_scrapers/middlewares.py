# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
import random
import time
import json
from pathlib import Path
from collections import defaultdict, deque
from urllib.parse import urlparse

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ReqScrapersSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # maching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class RotatingUserAgentMiddleware:
    """Middleware to rotate user agents for each request"""
    
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
    
    def process_request(self, request, spider):
        user_agent = random.choice(self.user_agents)
        request.headers['User-Agent'] = user_agent
        spider.logger.debug(f"Using User-Agent: {user_agent[:50]}...")
        return None


class ProxyRotationMiddleware:
    """Enhanced proxy rotation middleware with health monitoring"""
    
    def __init__(self):
        self.proxy_stats = defaultdict(lambda: {
            'requests': 0,
            'errors': 0,
            'last_used': 0,
            'blocked_until': 0,
            'consecutive_errors': 0
        })
        self.proxy_list = []
        self.current_proxy_index = 0
        self.load_proxies()
    
    def load_proxies(self):
        """Load proxies from proxies.json"""
        try:
            proxies_path = Path.cwd() / "proxies.json"
            if proxies_path.is_file():
                with proxies_path.open("r", encoding="utf-8-sig") as pf:
                    loaded = json.load(pf)
                    if isinstance(loaded, list):
                        self.proxy_list = [str(x).strip() for x in loaded if str(x).strip()]
                        print(f"Loaded {len(self.proxy_list)} proxies")
        except Exception as e:
            print(f"Failed to load proxies: {e}")
    
    def get_next_proxy(self):
        """Get the next available proxy"""
        if not self.proxy_list:
            return None
        
        current_time = time.time()
        available_proxies = []
        
        for i, proxy in enumerate(self.proxy_list):
            stats = self.proxy_stats[proxy]
            # Skip if proxy is temporarily blocked
            if stats['blocked_until'] > current_time:
                continue
            # Skip if too many consecutive errors
            if stats['consecutive_errors'] >= 5:
                continue
            available_proxies.append(i)
        
        if not available_proxies:
            # Reset all proxies if none are available
            for proxy in self.proxy_stats:
                self.proxy_stats[proxy]['consecutive_errors'] = 0
                self.proxy_stats[proxy]['blocked_until'] = 0
            available_proxies = list(range(len(self.proxy_list)))
        
        # Use round-robin with some randomization
        if available_proxies:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            while self.current_proxy_index not in available_proxies:
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        
        return self.proxy_list[self.current_proxy_index] if self.proxy_list else None
    
    def process_request(self, request, spider):
        proxy = self.get_next_proxy()
        if proxy:
            request.meta['proxy'] = f"http://{proxy}"
            # Track proxy usage
            self.proxy_stats[proxy]['requests'] += 1
            self.proxy_stats[proxy]['last_used'] = time.time()
            spider.logger.debug(f"Using proxy: {proxy}")
        return None
    
    def process_response(self, request, response, spider):
        proxy = request.meta.get('proxy', '').replace('http://', '')
        if proxy and proxy in self.proxy_stats:
            # Reset consecutive errors on successful response
            self.proxy_stats[proxy]['consecutive_errors'] = 0
            
            # Check for blocking indicators
            if response.status in [403, 429]:
                self.proxy_stats[proxy]['consecutive_errors'] += 1
                self.proxy_stats[proxy]['blocked_until'] = time.time() + (60 * 5)  # Block for 5 minutes
                spider.logger.warning(f"Proxy {proxy} appears blocked (status {response.status})")
        
        return response
    
    def process_exception(self, request, exception, spider):
        proxy = request.meta.get('proxy', '').replace('http://', '')
        if proxy and proxy in self.proxy_stats:
            self.proxy_stats[proxy]['errors'] += 1
            self.proxy_stats[proxy]['consecutive_errors'] += 1
            spider.logger.warning(f"Proxy {proxy} error: {exception}")
        return None


class RequestTrackingMiddleware:
    """Middleware to track requests and detect blocking patterns"""
    
    def __init__(self):
        self.domain_stats = defaultdict(lambda: {
            'requests': 0,
            'errors': 0,
            'blocked_requests': 0,
            'last_request': 0,
            'request_times': deque(maxlen=100)
        })
        self.blocking_patterns = {
            429: 'Rate Limited',
            403: 'Forbidden',
            503: 'Service Unavailable',
            502: 'Bad Gateway',
            504: 'Gateway Timeout'
        }
    
    def process_request(self, request, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        
        current_time = time.time()
        stats['requests'] += 1
        stats['last_request'] = current_time
        stats['request_times'].append(current_time)
        
        # Calculate request rate
        if len(stats['request_times']) > 1:
            time_diff = stats['request_times'][-1] - stats['request_times'][0]
            if time_diff > 0:
                rate = len(stats['request_times']) / time_diff
                if rate > 0.5:  # More than 0.5 requests per second
                    spider.logger.warning(f"High request rate detected for {domain}: {rate:.2f} req/s")
        
        return None
    
    def process_response(self, request, response, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        
        if response.status in self.blocking_patterns:
            stats['blocked_requests'] += 1
            blocking_type = self.blocking_patterns[response.status]
            spider.logger.warning(f"Blocking detected for {domain}: {blocking_type} (Status: {response.status})")
            
            # Log blocking statistics
            total_requests = stats['requests']
            blocked_ratio = stats['blocked_requests'] / total_requests if total_requests > 0 else 0
            spider.logger.info(f"Domain {domain} blocking ratio: {blocked_ratio:.2%} ({stats['blocked_requests']}/{total_requests})")
        
        return response
    
    def process_exception(self, request, exception, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        stats['errors'] += 1
        spider.logger.error(f"Exception for {domain}: {exception}")
        return None


class IntelligentRateLimitMiddleware:
    """Middleware for intelligent rate limiting based on response patterns"""
    
    def __init__(self):
        self.domain_stats = defaultdict(lambda: {
            'successful_requests': 0,
            'blocked_requests': 0,
            'last_blocked': 0,
            'consecutive_blocks': 0,
            'backoff_until': 0,
            'request_history': deque(maxlen=50)
        })
        self.blocking_codes = {403, 429, 503}
        self.backoff_multiplier = 2
        self.max_backoff = 300  # 5 minutes max
    
    def process_request(self, request, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        current_time = time.time()
        
        # Check if we're in backoff period
        if stats['backoff_until'] > current_time:
            wait_time = stats['backoff_until'] - current_time
            spider.logger.info(f"Rate limiting {domain}: waiting {wait_time:.1f}s")
            time.sleep(wait_time)
        
        # Record request
        stats['request_history'].append(current_time)
        
        return None
    
    def process_response(self, request, response, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        current_time = time.time()
        
        if response.status in self.blocking_codes:
            stats['blocked_requests'] += 1
            stats['last_blocked'] = current_time
            stats['consecutive_blocks'] += 1
            
            # Calculate backoff time
            backoff_time = min(
                (self.backoff_multiplier ** stats['consecutive_blocks']) * 10,  # Start with 10s
                self.max_backoff
            )
            stats['backoff_until'] = current_time + backoff_time
            
            spider.logger.warning(
                f"Rate limit triggered for {domain}: "
                f"backing off for {backoff_time}s (consecutive blocks: {stats['consecutive_blocks']})"
            )
        else:
            # Reset consecutive blocks on successful response
            if stats['consecutive_blocks'] > 0:
                spider.logger.info(f"Rate limit reset for {domain} after successful response")
            stats['consecutive_blocks'] = 0
            stats['successful_requests'] += 1
        
        return response


class SmartRetryMiddleware:
    """Middleware for smart retry strategies with exponential backoff"""
    
    def __init__(self):
        self.retry_stats = defaultdict(lambda: {
            'retry_count': 0,
            'last_retry': 0,
            'retry_delays': []
        })
        self.max_retries = 3
        self.base_delay = 5  # Base delay in seconds
    
    def process_exception(self, request, exception, spider):
        domain = urlparse(request.url).netloc
        stats = self.retry_stats[domain]
        
        if stats['retry_count'] >= self.max_retries:
            spider.logger.error(f"Max retries exceeded for {domain}: {exception}")
            return None
        
        # Calculate exponential backoff delay
        delay = self.base_delay * (2 ** stats['retry_count'])
        delay += random.uniform(0, delay * 0.1)  # Add jitter
        
        stats['retry_count'] += 1
        stats['last_retry'] = time.time()
        stats['retry_delays'].append(delay)
        
        spider.logger.info(f"Retrying {domain} in {delay:.1f}s (attempt {stats['retry_count']})")
        
        # Create a new request with delay
        new_request = request.replace(dont_filter=True)
        new_request.meta['retry_delay'] = delay
        
        return new_request


class ReqScrapersDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class RotatingUserAgentMiddleware:
    """Middleware to rotate user agents for each request"""
    
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
    
    def process_request(self, request, spider):
        user_agent = random.choice(self.user_agents)
        request.headers['User-Agent'] = user_agent
        spider.logger.debug(f"Using User-Agent: {user_agent[:50]}...")
        return None


class ProxyRotationMiddleware:
    """Enhanced proxy rotation middleware with health monitoring"""
    
    def __init__(self):
        self.proxy_stats = defaultdict(lambda: {
            'requests': 0,
            'errors': 0,
            'last_used': 0,
            'blocked_until': 0,
            'consecutive_errors': 0
        })
        self.proxy_list = []
        self.current_proxy_index = 0
        self.load_proxies()
    
    def load_proxies(self):
        """Load proxies from proxies.json"""
        try:
            proxies_path = Path.cwd() / "proxies.json"
            if proxies_path.is_file():
                with proxies_path.open("r", encoding="utf-8-sig") as pf:
                    loaded = json.load(pf)
                    if isinstance(loaded, list):
                        self.proxy_list = [str(x).strip() for x in loaded if str(x).strip()]
                        print(f"Loaded {len(self.proxy_list)} proxies")
        except Exception as e:
            print(f"Failed to load proxies: {e}")
    
    def get_next_proxy(self):
        """Get the next available proxy"""
        if not self.proxy_list:
            return None
        
        current_time = time.time()
        available_proxies = []
        
        for i, proxy in enumerate(self.proxy_list):
            stats = self.proxy_stats[proxy]
            # Skip if proxy is temporarily blocked
            if stats['blocked_until'] > current_time:
                continue
            # Skip if too many consecutive errors
            if stats['consecutive_errors'] >= 5:
                continue
            available_proxies.append(i)
        
        if not available_proxies:
            # Reset all proxies if none are available
            for proxy in self.proxy_stats:
                self.proxy_stats[proxy]['consecutive_errors'] = 0
                self.proxy_stats[proxy]['blocked_until'] = 0
            available_proxies = list(range(len(self.proxy_list)))
        
        # Use round-robin with some randomization
        if available_proxies:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            while self.current_proxy_index not in available_proxies:
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        
        return self.proxy_list[self.current_proxy_index] if self.proxy_list else None
    
    def process_request(self, request, spider):
        proxy = self.get_next_proxy()
        if proxy:
            request.meta['proxy'] = f"http://{proxy}"
            # Track proxy usage
            self.proxy_stats[proxy]['requests'] += 1
            self.proxy_stats[proxy]['last_used'] = time.time()
            spider.logger.debug(f"Using proxy: {proxy}")
        return None
    
    def process_response(self, request, response, spider):
        proxy = request.meta.get('proxy', '').replace('http://', '')
        if proxy and proxy in self.proxy_stats:
            # Reset consecutive errors on successful response
            self.proxy_stats[proxy]['consecutive_errors'] = 0
            
            # Check for blocking indicators
            if response.status in [403, 429]:
                self.proxy_stats[proxy]['consecutive_errors'] += 1
                self.proxy_stats[proxy]['blocked_until'] = time.time() + (60 * 5)  # Block for 5 minutes
                spider.logger.warning(f"Proxy {proxy} appears blocked (status {response.status})")
        
        return response
    
    def process_exception(self, request, exception, spider):
        proxy = request.meta.get('proxy', '').replace('http://', '')
        if proxy and proxy in self.proxy_stats:
            self.proxy_stats[proxy]['errors'] += 1
            self.proxy_stats[proxy]['consecutive_errors'] += 1
            spider.logger.warning(f"Proxy {proxy} error: {exception}")
        return None


class RequestTrackingMiddleware:
    """Middleware to track requests and detect blocking patterns"""
    
    def __init__(self):
        self.domain_stats = defaultdict(lambda: {
            'requests': 0,
            'errors': 0,
            'blocked_requests': 0,
            'last_request': 0,
            'request_times': deque(maxlen=100)
        })
        self.blocking_patterns = {
            429: 'Rate Limited',
            403: 'Forbidden',
            503: 'Service Unavailable',
            502: 'Bad Gateway',
            504: 'Gateway Timeout'
        }
    
    def process_request(self, request, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        
        current_time = time.time()
        stats['requests'] += 1
        stats['last_request'] = current_time
        stats['request_times'].append(current_time)
        
        # Calculate request rate
        if len(stats['request_times']) > 1:
            time_diff = stats['request_times'][-1] - stats['request_times'][0]
            if time_diff > 0:
                rate = len(stats['request_times']) / time_diff
                if rate > 0.5:  # More than 0.5 requests per second
                    spider.logger.warning(f"High request rate detected for {domain}: {rate:.2f} req/s")
        
        return None
    
    def process_response(self, request, response, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        
        if response.status in self.blocking_patterns:
            stats['blocked_requests'] += 1
            blocking_type = self.blocking_patterns[response.status]
            spider.logger.warning(f"Blocking detected for {domain}: {blocking_type} (Status: {response.status})")
            
            # Log blocking statistics
            total_requests = stats['requests']
            blocked_ratio = stats['blocked_requests'] / total_requests if total_requests > 0 else 0
            spider.logger.info(f"Domain {domain} blocking ratio: {blocked_ratio:.2%} ({stats['blocked_requests']}/{total_requests})")
        
        return response
    
    def process_exception(self, request, exception, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        stats['errors'] += 1
        spider.logger.error(f"Exception for {domain}: {exception}")
        return None


class IntelligentRateLimitMiddleware:
    """Middleware for intelligent rate limiting based on response patterns"""
    
    def __init__(self):
        self.domain_stats = defaultdict(lambda: {
            'successful_requests': 0,
            'blocked_requests': 0,
            'last_blocked': 0,
            'consecutive_blocks': 0,
            'backoff_until': 0,
            'request_history': deque(maxlen=50)
        })
        self.blocking_codes = {403, 429, 503}
        self.backoff_multiplier = 2
        self.max_backoff = 300  # 5 minutes max
    
    def process_request(self, request, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        current_time = time.time()
        
        # Check if we're in backoff period
        if stats['backoff_until'] > current_time:
            wait_time = stats['backoff_until'] - current_time
            spider.logger.info(f"Rate limiting {domain}: waiting {wait_time:.1f}s")
            time.sleep(wait_time)
        
        # Record request
        stats['request_history'].append(current_time)
        
        return None
    
    def process_response(self, request, response, spider):
        domain = urlparse(request.url).netloc
        stats = self.domain_stats[domain]
        current_time = time.time()
        
        if response.status in self.blocking_codes:
            stats['blocked_requests'] += 1
            stats['last_blocked'] = current_time
            stats['consecutive_blocks'] += 1
            
            # Calculate backoff time
            backoff_time = min(
                (self.backoff_multiplier ** stats['consecutive_blocks']) * 10,  # Start with 10s
                self.max_backoff
            )
            stats['backoff_until'] = current_time + backoff_time
            
            spider.logger.warning(
                f"Rate limit triggered for {domain}: "
                f"backing off for {backoff_time}s (consecutive blocks: {stats['consecutive_blocks']})"
            )
        else:
            # Reset consecutive blocks on successful response
            if stats['consecutive_blocks'] > 0:
                spider.logger.info(f"Rate limit reset for {domain} after successful response")
            stats['consecutive_blocks'] = 0
            stats['successful_requests'] += 1
        
        return response


class SmartRetryMiddleware:
    """Middleware for smart retry strategies with exponential backoff"""
    
    def __init__(self):
        self.retry_stats = defaultdict(lambda: {
            'retry_count': 0,
            'last_retry': 0,
            'retry_delays': []
        })
        self.max_retries = 3
        self.base_delay = 5  # Base delay in seconds
    
    def process_exception(self, request, exception, spider):
        domain = urlparse(request.url).netloc
        stats = self.retry_stats[domain]
        
        if stats['retry_count'] >= self.max_retries:
            spider.logger.error(f"Max retries exceeded for {domain}: {exception}")
            return None
        
        # Calculate exponential backoff delay
        delay = self.base_delay * (2 ** stats['retry_count'])
        delay += random.uniform(0, delay * 0.1)  # Add jitter
        
        stats['retry_count'] += 1
        stats['last_retry'] = time.time()
        stats['retry_delays'].append(delay)
        
        spider.logger.info(f"Retrying {domain} in {delay:.1f}s (attempt {stats['retry_count']})")
        
        # Create a new request with delay
        new_request = request.replace(dont_filter=True)
        new_request.meta['retry_delay'] = delay
        
        return new_request
