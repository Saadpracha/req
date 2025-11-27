# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

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

        # Enhanced debugging: Log proxy and request details
        proxy = request.meta.get("proxy", "none")
        neq = request.meta.get("neq", "unknown")
        url = request.url
        
        spider.logger.debug(f"[MIDDLEWARE] process_request: NEQ={neq}, URL={url}, Proxy={proxy}")
        spider.logger.debug(f"[MIDDLEWARE] Request headers: {dict(request.headers)}")
        
        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Enhanced debugging: Log response details
        proxy = request.meta.get("proxy", "none")
        neq = request.meta.get("neq", "unknown")
        url = request.url
        status = response.status
        body_size = len(response.body) if response.body else 0
        
        spider.logger.debug(f"[MIDDLEWARE] process_response: NEQ={neq}, URL={url}, Status={status}, Proxy={proxy}, BodySize={body_size}")
        
        if status != 200:
            spider.logger.warning(f"[MIDDLEWARE] Non-200 response: NEQ={neq}, Status={status}, Proxy={proxy}, URL={url}")
            if response.body:
                spider.logger.debug(f"[MIDDLEWARE] Response body preview (first 500 chars): {response.body[:500].decode('utf-8', errors='ignore')}")

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Enhanced debugging: Log exception details
        proxy = request.meta.get("proxy", "none")
        neq = request.meta.get("neq", "unknown")
        url = request.url
        exception_type = type(exception).__name__
        exception_msg = str(exception)
        
        spider.logger.error(f"[MIDDLEWARE] process_exception: NEQ={neq}, URL={url}, Proxy={proxy}, Exception={exception_type}: {exception_msg}")

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
