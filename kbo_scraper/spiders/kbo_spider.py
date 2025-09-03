import scrapy
import pandas as pd
from kbo_scraper.items import KboScraperItem

class KboSpider(scrapy.Spider):
    name = "kbo_spider"

    def start_requests(self):
        # Lecture CSV (fichier test avec 10 lignes)
        df = pd.read_csv("enterprise_test.csv")
        for numero in df["EnterpriseNumber"].head(1):  # pour tester, on prend juste le premier
            numero_clean = numero.replace(".", "")  # enlever les points
            url = f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={numero_clean}"
            yield scrapy.Request(url, callback=self.parse, meta={"numero": numero})

    def parse(self, response):
        item = KboScraperItem()
        item["enterprise_number"] = response.meta["numero"]

        # Extraction du statut
        status = response.xpath('//div[@id="table"]/table[1]/tbody/tr[3]/td[2]/strong/span/text()').get()

        item["status"] = status.strip() if status else None

        yield item