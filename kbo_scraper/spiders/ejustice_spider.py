import scrapy
import pandas as pd
from kbo_scraper.items import KboScraperItem
import logging
import re
import json
from urllib.parse import urljoin, parse_qs, urlparse
import pymongo


class EjusticeSpider(scrapy.Spider):
    name = "ejustice_spider"

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-BE,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        },
        'DOWNLOAD_DELAY': 3,
        'RANDOMIZE_DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def __init__(self, *args, **kwargs):
        super(EjusticeSpider, self).__init__(*args, **kwargs)
        # Connexion MongoDB pour récupérer les numéros d'entreprise
        self.mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
        self.db = self.mongo_client["kbo_db"]

    def start_requests(self):
        """
        Récupère les numéros d'entreprise depuis MongoDB ou CSV
        et génère les URLs pour ejustice
        """
        try:
            # Option 1: Récupérer depuis MongoDB
            entreprises = self.db.entreprises.find({}, {"enterprise_number": 1})
            enterprise_numbers = [ent["enterprise_number"] for ent in entreprises]

            # Si MongoDB est vide, utiliser le CSV
            if not enterprise_numbers:
                df = pd.read_csv("enterprise_test.csv")
                enterprise_numbers = df["EnterpriseNumber"].tolist()

        except Exception as e:
            self.logger.warning(f"Erreur MongoDB: {e}. Utilisation du CSV.")
            df = pd.read_csv("enterprise_test.csv")
            enterprise_numbers = df["EnterpriseNumber"].tolist()

        for numero in enterprise_numbers[:10]:  # Limiter pour les tests
            # Nettoyer le numéro (enlever les points et le zéro en tête)
            numero_clean = numero.replace(".", "").strip()
            if numero_clean.startswith("0"):
                numero_clean = numero_clean[1:]

            url = f"https://www.ejustice.just.fgov.be/cgi_tsv/article.pl?language=fr&btw_search={numero_clean}"

            self.logger.info(f"Requesting ejustice URL: {url}")

            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={"enterprise_number": numero},
                dont_filter=True,
                errback=self.handle_error
            )

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure}")
        if hasattr(failure.value, 'response'):
            self.logger.error(f"Response status: {failure.value.response.status}")

    def clean_text(self, text):
        """Nettoie le texte des espaces superflus"""
        if text:
            return re.sub(r'\s+', ' ', text.strip())
        return None

    def extract_publication_data(self, row, enterprise_number):
        """
        Extrait les données d'une publication depuis une ligne du tableau
        """
        publication_data = {
            'enterprise_number': enterprise_number,
            'publication_number': None,
            'title': None,
            'code': None,
            'address': None,
            'publication_type': None,
            'publication_date': None,
            'reference': None,
            'image_url': None,
            'detail_url': None
        }

        try:
            # Colonne 1: Numéro de publication (souvent un lien)
            pub_number_cell = row.xpath('.//td[1]')
            if pub_number_cell:
                # Chercher un lien avec le numéro
                pub_link = pub_number_cell.xpath('.//a/@href').get()
                pub_number_text = pub_number_cell.xpath('.//text()').get()

                if pub_link:
                    publication_data['detail_url'] = urljoin('https://www.ejustice.just.fgov.be/', pub_link)

                if pub_number_text:
                    publication_data['publication_number'] = self.clean_text(pub_number_text)

            # Colonne 2: Titre et informations
            title_cell = row.xpath('.//td[2]')
            if title_cell:
                title_texts = title_cell.xpath('.//text()').getall()
                if title_texts:
                    full_title = ' '.join([t.strip() for t in title_texts if t.strip()])
                    publication_data['title'] = self.clean_text(full_title)

            # Colonne 3: Code (si présent)
            code_cell = row.xpath('.//td[3]')
            if code_cell:
                code_text = code_cell.xpath('.//text()').get()
                if code_text:
                    publication_data['code'] = self.clean_text(code_text)

            # Colonne 4: Adresse (si présente)
            address_cell = row.xpath('.//td[4]')
            if address_cell:
                address_texts = address_cell.xpath('.//text()').getall()
                if address_texts:
                    full_address = ' '.join([t.strip() for t in address_texts if t.strip()])
                    publication_data['address'] = self.clean_text(full_address)

            # Colonne 5: Type de publication
            type_cell = row.xpath('.//td[5]')
            if type_cell:
                type_text = type_cell.xpath('.//text()').get()
                if type_text:
                    publication_data['publication_type'] = self.clean_text(type_text)

            # Colonne 6: Date de publication
            date_cell = row.xpath('.//td[6]')
            if date_cell:
                date_text = date_cell.xpath('.//text()').get()
                if date_text:
                    publication_data['publication_date'] = self.clean_text(date_text)

            # Colonne 7: Référence
            ref_cell = row.xpath('.//td[7]')
            if ref_cell:
                ref_text = ref_cell.xpath('.//text()').get()
                if ref_text:
                    publication_data['reference'] = self.clean_text(ref_text)

            # Chercher une image (peut être dans n'importe quelle colonne)
            image_link = row.xpath('.//img/@src').get()
            if image_link:
                publication_data['image_url'] = urljoin('https://www.ejustice.just.fgov.be/', image_link)

        except Exception as e:
            self.logger.error(f"Erreur extraction publication: {e}")

        return publication_data

    def parse(self, response):
        enterprise_number = response.meta['enterprise_number']

        # Sauvegarde brute pour inspection
        filename = f"raw_{enterprise_number.replace('.', '')}.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)

        self.logger.info(f"Fichier brut sauvegardé: {filename}")
        self.logger.info(f"Parsing ejustice page for enterprise: {enterprise_number}")

        # Extraire la zone principale
        main = response.xpath("//main").get()
        if not main:
            self.logger.info(f"Aucune section <main> trouvée pour {enterprise_number}")
            return

        # Découper en blocs séparés par <hr>
        blocks = response.xpath("//main").getall()[0].split("<hr>")
        publications = []

        for block in blocks:
            lines = re.split(r"<br\s*/?>", block)
            lines = [self.clean_text(re.sub("<.*?>", "", l)) for l in lines if l.strip()]

            if not lines:
                continue

            publication_data = {
                "enterprise_number": enterprise_number,
                "publication_number": None,
                "title": None,
                "code": None,
                "address": None,
                "publication_type": None,
                "publication_date": None,
                "reference": None,
            }

            # Exemple basique : on mappe les premières lignes
            if len(lines) >= 1:
                publication_data["title"] = lines[0]
            if len(lines) >= 2:
                publication_data["address"] = lines[1]
            if len(lines) >= 3:
                publication_data["publication_number"] = lines[2]
            if len(lines) >= 4:
                publication_data["publication_type"] = lines[3]
            if len(lines) >= 5:
                publication_data["reference"] = lines[4]

            publications.append(publication_data)
            yield self.create_publication_item(publication_data)

        self.logger.info(f"Trouvé {len(publications)} publications pour {enterprise_number}")

        # Pagination
        next_page = response.xpath('//a[contains(text(), "Suivant") or contains(text(), "Next")]/@href').get()
        if next_page:
            next_url = urljoin(response.url, next_page)
            yield scrapy.Request(
                next_url,
                callback=self.parse,
                meta={'enterprise_number': enterprise_number}
            )

    def parse_publication_detail(self, response):
        """
        Parse les détails d'une publication spécifique
        """
        publication_data = response.meta['publication_data']
        enterprise_number = response.meta['enterprise_number']

        # Enrichir les données avec les détails de la page
        try:
            # Chercher plus d'informations sur la page détaillée
            full_text = response.xpath('//body//text()').getall()
            full_content = ' '.join([t.strip() for t in full_text if t.strip()])

            # Chercher des images supplémentaires
            additional_images = response.xpath('//img/@src').getall()
            if additional_images and not publication_data['image_url']:
                publication_data['image_url'] = urljoin(response.url, additional_images[0])

            # Ajouter le contenu complet si nécessaire
            publication_data['full_content'] = self.clean_text(full_content[:1000])  # Limiter la taille

        except Exception as e:
            self.logger.error(f"Erreur parsing détails: {e}")

        yield self.create_publication_item(publication_data)

    def create_publication_item(self, publication_data):
        """
        Crée un item Scrapy pour une publication
        """
        item = KboScraperItem()

        # Utiliser l'enterprise_number comme clé principale
        item['enterprise_number'] = publication_data['enterprise_number']

        # Créer un champ spécial pour les publications
        item['moniteur_publications'] = json.dumps([publication_data], ensure_ascii=False)

        return item

    def update_enterprise_with_publications(self, enterprise_number, publications):
        """
        Met à jour l'entreprise dans MongoDB avec ses publications
        """
        try:
            self.db.entreprises.update_one(
                {"enterprise_number": enterprise_number},
                {
                    "$set": {"moniteur_publications": publications},
                    "$currentDate": {"moniteur_last_updated": True}
                },
                upsert=True
            )
        except Exception as e:
            self.logger.error(f"Erreur mise à jour MongoDB: {e}")

    def closed(self, reason):
        """Ferme la connexion MongoDB à la fin"""
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()