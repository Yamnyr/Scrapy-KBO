import scrapy
import re
import json
from urllib.parse import urljoin
import pymongo
import logging

DATE_NUM_RE = re.compile(r'(\d{4}-\d{2}-\d{2})\s*/\s*(\d+)')
ADDRESS_HINT = re.compile(r'\b\d{4}\b')  # code postal belge


class EjusticeSpider(scrapy.Spider):
    name = "ejustice_spider"
    allowed_domains = ["www.ejustice.just.fgov.be", "ejustice.just.fgov.be"]

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS': 1,
        'FEED_EXPORT_ENCODING': 'utf-8',
        'LOG_LEVEL': 'INFO',
    }

    def __init__(self, limit=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit = int(limit) if str(limit).isdigit() else 10
        self.mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
        self.db = self.mongo_client["kbo_db"]

    def start_requests(self):
        entreprises = self.db.entreprises.find({}, {"enterprise_number": 1})
        enterprise_numbers = [ent["enterprise_number"] for ent in entreprises]
        enterprise_numbers = enterprise_numbers[: self.limit]

        for numero in enterprise_numbers:
            numero_clean = numero.replace(".", "").strip()
            if numero_clean.startswith("0"):
                numero_clean = numero_clean[1:]

            url = f"https://www.ejustice.just.fgov.be/cgi_tsv/list.pl?btw={numero_clean}"
            self.logger.info(f"Requesting: {url}")
            yield scrapy.Request(
                url,
                callback=self.parse_list,
                meta={"enterprise_number": numero},
                dont_filter=True,
            )

    def parse_list(self, response):
        enterprise_number = response.meta["enterprise_number"]
        items = response.xpath('//div[@class="list-item"]')
        publications = []

        for item in items:
            content = item.xpath('.//div[@class="list-item--content"]')

            # 1) Numéro de publication (position dans la liste)
            subtitle_text = content.xpath('.//p[contains(@class,"list-item--subtitle")]//text()').getall()
            subtitle_text = [t.strip() for t in subtitle_text if t.strip()]
            publication_code = subtitle_text[-1] if subtitle_text else None  # CV, SPRL, ...

            # 2) Titre de la publication
            title_block = content.xpath('.//a[contains(@class,"list-item--title")]').get()
            title_lines = content.xpath('.//a[contains(@class,"list-item--title")]//text()').getall()
            title_lines = [line.strip() for line in title_lines if line.strip()]

            address = title_lines[0] if len(title_lines) > 0 else None
            enterprise_num = title_lines[1] if len(title_lines) > 1 else None
            type_pub = title_lines[2] if len(title_lines) > 2 else None

            date_ref_match = re.search(r'(\d{4}-\d{2}-\d{2})\s*/\s*(\d+)', ' '.join(title_lines))
            publication_date, publication_ref = (date_ref_match.groups() if date_ref_match else (None, None))

            # 3) URL de l'image (PDF)
            pdf_href = content.xpath('.//a[@class="standard"]/@href').get()
            pdf_url = urljoin(response.url, pdf_href) if pdf_href else None

            # 4) Lien détail
            detail_link = content.xpath('.//a[contains(@class,"read-more")]/@href').get()
            detail_url = urljoin(response.url, detail_link) if detail_link else None

            publication = {
                "enterprise_number": enterprise_number,
                "publication_code": publication_code,
                "address": address,
                "type_publication": type_pub,
                "publication_date": publication_date,
                "publication_ref": publication_ref,
                "pdf_url": pdf_url,
                "detail_url": detail_url
            }
            publications.append(publication)

        if publications:
            try:
                self.db.entreprises.update_one(
                    {"enterprise_number": enterprise_number},
                    {"$set": {"moniteur_publications": publications},
                     "$currentDate": {"moniteur_last_updated": True}},
                    upsert=True,
                )
                self.logger.info(f"MàJ Mongo : {enterprise_number} ({len(publications)} pubs)")
            except Exception as e:
                self.logger.error(f"Erreur update Mongo: {e}")

            yield {
                "enterprise_number": enterprise_number,
                "moniteur_publications": json.dumps(publications, ensure_ascii=False),
            }

    def closed(self, reason):
        if hasattr(self, "mongo_client"):
            self.mongo_client.close()
