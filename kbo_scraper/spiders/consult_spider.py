# kbo_scraper/spiders/consult_spider.py
import scrapy
from kbo_scraper.items import KboScraperItem


class ConsultSpider(scrapy.Spider):
    name = "consult_spider"
    allowed_domains = ["consult.cbso.nbb.be"]

    custom_settings = {
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, enterprise_numbers=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Accepter les numéros d'entreprise en paramètre
        if enterprise_numbers:
            if isinstance(enterprise_numbers, str):
                # Si c'est une string, on assume que c'est séparé par des virgules
                self.enterprise_numbers = [num.strip() for num in enterprise_numbers.split(',')]
            elif isinstance(enterprise_numbers, list):
                self.enterprise_numbers = enterprise_numbers
            else:
                self.enterprise_numbers = []
        else:
            self.enterprise_numbers = []

        self.logger.info(f"Spider initialisé avec {len(self.enterprise_numbers)} numéros d'entreprise")

    def start_requests(self):
        if not self.enterprise_numbers:
            self.logger.warning("Aucun numéro d'entreprise fourni. Spider arrêté.")
            return

        for numero in self.enterprise_numbers:
            numero_clean = numero.replace(".", "").strip()
            api_url = (
                "https://consult.cbso.nbb.be/api/rs-consult/published-deposits"
                f"?page=0&size=50&enterpriseNumber={numero_clean}"
                "&sort=periodEndDate,desc&sort=depositDate,desc"
            )
            yield scrapy.Request(
                api_url,
                callback=self.parse_api,
                meta={"enterprise_number": numero, "url": api_url},
                errback=self.errback,
            )

    def parse_api(self, response):
        enterprise_number = response.meta["enterprise_number"]
        data = response.json()

        deposits = []
        for dep in data.get("content", []):
            deposits.append({
                "title": dep.get("modelName", "").strip(),
                "reference": dep.get("reference", "").strip(),
                "start_date": dep.get("depositDate", "").strip(),
                "end_date": dep.get("periodEndDate", "").strip(),
                "language": dep.get("language", "").strip(),
            })

        item = KboScraperItem()
        item["enterprise_number"] = enterprise_number
        item["url"] = response.meta["url"]
        item["deposits"] = deposits

        yield item

    def errback(self, failure):
        request = failure.request
        self.logger.error(f"Erreur pour {request.url}: {repr(failure.value)}")