import scrapy
import re
import json
from urllib.parse import urljoin
import pymongo


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
        # Lecture Mongo UNIQUEMENT pour récupérer les numéros (pas d'écriture ici)
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
        db = mongo_client["kbo_db"]
        entreprises = db.entreprises.find({}, {"enterprise_number": 1})
        self.enterprise_numbers = [ent["enterprise_number"] for ent in entreprises][: self.limit]
        mongo_client.close()

    def start_requests(self):
        for numero in self.enterprise_numbers:
            numero_clean = numero.replace(".", "").strip()
            if numero_clean.startswith("0"):
                numero_clean = numero_clean[1:]
            url = f"https://www.ejustice.just.fgov.be/cgi_tsv/list.pl?btw={numero_clean}"
            yield scrapy.Request(
                url,
                callback=self.parse_list,
                meta={
                    "enterprise_number": numero,
                    # publications_acc est l'accumulateur qui va suivre toutes les pages
                    "publications_acc": []
                },
                dont_filter=True,
            )

    def parse_list(self, response):
        enterprise_number = response.meta["enterprise_number"]
        publications_acc = response.meta.get("publications_acc", [])
        visited_pages = response.meta.get("visited_pages", set())

        # Numéro de page courante
        page_match = re.search(r'page=(\d+)', response.url)
        current_page = int(page_match.group(1)) if page_match else 1
        visited_pages.add(current_page)

        # Récupération des publications
        items = response.xpath('//div[@class="list-item"]')
        if not items:
            self.logger.info(f"Page vide détectée -> fin pagination pour {enterprise_number}")
            if publications_acc:
                yield {
                    "enterprise_number": enterprise_number,
                    "moniteur_publications": json.dumps(publications_acc, ensure_ascii=False),
                }
            return

        for item in items:
            content = item.xpath('.//div[@class="list-item--content"]')

            subtitle_text = content.xpath('.//p[contains(@class,"list-item--subtitle")]//text()').getall()
            subtitle_text = [t.strip() for t in subtitle_text if t.strip()]
            publication_code = subtitle_text[-1] if subtitle_text else None

            title_lines = content.xpath('.//a[contains(@class,"list-item--title")]//text()').getall()
            title_lines = [line.strip() for line in title_lines if line.strip()]

            address = title_lines[0] if len(title_lines) > 0 else None
            type_pub = title_lines[2] if len(title_lines) > 2 else None

            date_ref_match = re.search(r'(\d{4}-\d{2}-\d{2})\s*/\s*(\d+)', ' '.join(title_lines))
            publication_date, publication_ref = (date_ref_match.groups() if date_ref_match else (None, None))

            pdf_href = content.xpath('.//a[@class="standard"]/@href').get()
            pdf_url = urljoin(response.url, pdf_href) if pdf_href else None

            detail_link = content.xpath('.//a[contains(@class,"read-more")]/@href').get()
            detail_url = urljoin(response.url, detail_link) if detail_link else None

            title = type_pub or ' - '.join(title_lines) or publication_code or address or ""
            publication_number = publication_ref or publication_code or None

            publications_acc.append({
                "enterprise_number": enterprise_number,
                "title": title,
                "publication_number": publication_number,
                "publication_date": publication_date,
                "address": address,
                "type_publication": type_pub,
                "publication_code": publication_code,
                "publication_ref": publication_ref,
                "pdf_url": pdf_url,
                "detail_url": detail_url
            })

        # Pagination
        next_page = response.xpath(
            '//div[contains(@class,"pagination-container")]//a[contains(@class,"pagination-next")]/@href'
        ).get()

        if next_page:
            next_url = urljoin(response.url, next_page)

            # Numéro de la prochaine page
            next_match = re.search(r'page=(\d+)', next_url)
            next_num = int(next_match.group(1)) if next_match else None

            # ✅ Stop si déjà visité (boucle) ou trop loin
            if next_num and next_num in visited_pages:
                self.logger.info(f"Boucle détectée -> fin pagination pour {enterprise_number}")
            else:
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_list,
                    meta={
                        "enterprise_number": enterprise_number,
                        "publications_acc": publications_acc,
                        "visited_pages": visited_pages
                    },
                    dont_filter=True
                )
                return

        # Si pas de next_page OU boucle détectée → yield final
        if publications_acc:
            yield {
                "enterprise_number": enterprise_number,
                "moniteur_publications": json.dumps(publications_acc, ensure_ascii=False),
            }
        else:
            self.logger.info(f"Aucune publication trouvée pour {enterprise_number} (toutes pages).")
