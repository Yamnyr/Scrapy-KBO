# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
import random

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class KboScraperSpiderMiddleware:
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

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn't have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class KboScraperDownloaderMiddleware:
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


# 🆕 Middleware pour rotation des User-Agents
class RotateUserAgentMiddleware:
    """Middleware pour faire tourner les User-Agents"""

    def __init__(self, user_agent_list):
        self.user_agent_list = user_agent_list

    @classmethod
    def from_crawler(cls, crawler):
        user_agent_list = crawler.settings.get('USER_AGENT_LIST', [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        ])
        return cls(user_agent_list)

    def process_request(self, request, spider):
        if self.user_agent_list:
            ua = random.choice(self.user_agent_list)
            request.headers['User-Agent'] = ua
        return None


# 🆕 Middleware spécial pour ejustice avec retry intelligent
class EjusticeRetryMiddleware:
    """Middleware pour gérer les erreurs spécifiques à ejustice"""

    def __init__(self):
        self.retry_codes = [500, 502, 503, 504, 408, 429]

    def process_response(self, request, response, spider):
        if spider.name == "ejustice_spider":
            # Vérifier si la page contient des erreurs spécifiques
            if response.status == 200:
                body_text = response.text.lower()
                if any(error in body_text for error in ['erreur', 'error', 'unavailable', 'maintenance']):
                    spider.logger.warning(f"Page avec erreur détectée: {response.url}")
                    # On peut décider de retry ou ignorer

            # Ajouter un délai supplémentaire pour ejustice
            if hasattr(request, 'meta'):
                request.meta['download_delay'] = 4

        return response


# 🆕 Middleware pour detecter et gérer les CAPTCHAs
class CaptchaDetectionMiddleware:
    """Détecte les CAPTCHAs et arrête le spider si nécessaire"""

    def process_response(self, request, response, spider):
        if response.status == 200:
            body_text = response.text.lower()
            captcha_indicators = [
                'captcha', 'recaptcha', 'verification',
                'robot', 'blocked', 'verify you are human'
            ]

            if any(indicator in body_text for indicator in captcha_indicators):
                spider.logger.error(f"CAPTCHA détecté sur {response.url}")
                spider.logger.error("Arrêt du spider pour éviter le blocage")
                spider.crawler.engine.close_spider(spider, 'captcha_detected')

        return response