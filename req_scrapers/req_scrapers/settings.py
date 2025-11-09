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
# CONCURRENT_REQUESTS = 8
# CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1
RANDOMIZE_DOWNLOAD_DELAY = True
ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED = True
# DOWNLOAD_DELAY = 1

# Disable cookies (avoid server-side tracking linkage across requests)
# COOKIES_ENABLED = False

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
# Keep defaults; consider adding UA rotation middleware if needed
#DOWNLOADER_MIDDLEWARES = {
#    "req_scrapers.middlewares.ReqScrapersDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# By default, rely on Scrapy's feed export (-o). To enable immediate CSV appends,
# uncomment the pipeline below and set IMMEDIATE_CSV_PATH in your settings or via -s.
ITEM_PIPELINES = {
    # Enrich first so CSV has AI fields
    "req_scrapers.pipelines.AIEnrichmentPipeline": 250,
    # Optional immediate CSV append if you set IMMEDIATE_CSV_PATH
    # "req_scrapers.pipelines.ImmediateCSVPipeline": 300,
}

# IMMEDIATE_CSV_PATH = "path/to/your.csv"  # enable only if you want immediate writes

# Enable and configure the AutoThrottle extension (reduces request rate on load)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# # The initial download delay
# AUTOTHROTTLE_START_DELAY = 1.0
# # The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 10.0
# # The average number of requests Scrapy should be sending in parallel to
# # each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 0.5
# # Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Retry & timeouts (avoid hammering while handling transient failures)
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 408, 429]
DOWNLOAD_TIMEOUT = 30

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8-sig"
LOG_LEVEL = "DEBUG"
FEED_EXPORTERS = {
    "csv": "req_scrapers.exporters.QuotedCsvItemExporter",
}
