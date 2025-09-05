# kbo_scraper/spiders/consult_spider.py
import scrapy
import pymongo
from kbo_scraper.items import KboScraperItem


class ConsultSpider(scrapy.Spider):
    name = "consult_spider"
    allowed_domains = ["consult.cbso.nbb.be"]

    custom_settings = {
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, limit=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit = int(limit) if str(limit).isdigit() else 10

        # Connexion MongoDB pour récupérer les numéros d'entreprise
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
        db = mongo_client["kbo_db"]
        entreprises = db.entreprises.find({}, {"enterprise_number": 1})
        self.enterprise_numbers = [
            ent["enterprise_number"] for ent in entreprises
        ][: self.limit]
        mongo_client.close()

    def start_requests(self):
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
