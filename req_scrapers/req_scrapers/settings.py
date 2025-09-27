# Scrapy settings for req_scrapers project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "req_scrapers"

SPIDER_MODULES = ["req_scrapers.spiders"]
NEWSPIDER_MODULE = "req_scrapers.spiders"

# ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "req_scrapers (+http://www.yourdomain.com)"

# Obey robots.txt rules
# ROBOTSTXT_OBEY = True

# Concurrency and throttling settings (be polite; reduce ban risk)
# Use very conservative concurrency and a small randomized delay
CONCURRENT_REQUESTS = 8  # Reduced from 8 to be more conservative
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1.5  # Increased from 2 to 3 seconds
RANDOMIZE_DOWNLOAD_DELAY = True

# Enable cookies for session management (handled by comprehensive middleware)
# COOKIES_ENABLED = True

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers (set language; keep UA middleware default)
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "req_scrapers.middlewares.ReqScrapersSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    "req_scrapers.comprehensive_middleware.ComprehensiveAntiDetectionMiddleware": 400,
    "req_scrapers.middlewares.RequestTrackingMiddleware": 600,
    "req_scrapers.middlewares.IntelligentRateLimitMiddleware": 700,
    # "req_scrapers.middlewares.SmartRetryMiddleware": 800,  # Disabled - handled manually
    "req_scrapers.middlewares.ReqScrapersDownloaderMiddleware": 543,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# By default, rely on Scrapy's feed export (-o). To enable immediate CSV appends,
# uncomment the pipeline below and set IMMEDIATE_CSV_PATH in your settings or via -s.
# ITEM_PIPELINES = {
#     "req_scrapers.pipelines.ImmediateCSVPipeline": 300,
# }

# IMMEDIATE_CSV_PATH = "path/to/your.csv"  # enable only if you want immediate writes

# Enable and configure the AutoThrottle extension (reduces request rate on load)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# # The initial download delay
# AUTOTHROTTLE_START_DELAY = 1.0  # Increased from 1.0
# # The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 10.0  # Increased from 10.0
# # The average number of requests Scrapy should be sending in parallel to
# # each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 0.3  # Reduced from 0.5 to be more conservative
# # Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = True  # Enable for monitoring

# Retry & timeouts (avoid hammering while handling transient failures)
RETRY_ENABLED = True
RETRY_TIMES = 3  # Reduced from 5 to avoid excessive retries
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]  # Removed 400, 403, 404 as they're likely permanent
DOWNLOAD_TIMEOUT = 30  # Increased timeout
# DOWNLOAD_WARNSIZE = 33554432  # 32MB warning size
# DOWNLOAD_MAXSIZE = 52428800  # 50MB max size

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"

# Additional anti-detection settings
ROBOTSTXT_OBEY = False  # Disable robots.txt to avoid detection patterns
TELNETCONSOLE_ENABLED = False  # Disable telnet console for security

# DNS settings for better performance
# DNSCACHE_ENABLED = True
# DNSCACHE_SIZE = 10000
# DNS_TIMEOUT = 60

# Memory settings
# MEMDEBUG_ENABLED = True
# MEMUSAGE_ENABLED = True
# MEMUSAGE_LIMIT_MB = 2048
# MEMUSAGE_WARNING_MB = 1024

# Additional headers to appear more like a real browser
DEFAULT_REQUEST_HEADERS.update({
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
})
