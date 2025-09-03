import scrapy
import pandas as pd
from kbo_scraper.items import KboScraperItem
import logging


class KboSpider(scrapy.Spider):
    name = "kbo_spider"

    # Ajouter des headers pour éviter la détection de bot
    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-BE,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        },
        'DOWNLOAD_DELAY': 2,  # Délai entre les requêtes
        'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
    }

    def start_requests(self):
        # Lecture CSV (fichier test avec 10 lignes)
        df = pd.read_csv("enterprise_test.csv")

        # Tester avec plusieurs entreprises pour vérifier
        for numero in df["EnterpriseNumber"].head(3):  # Tester les 3 premiers
            numero_clean = numero.replace(".", "")  # enlever les points
            url = f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={numero_clean}"

            # Log pour debug
            self.logger.info(f"Requesting URL: {url}")

            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={"numero": numero},
                dont_filter=True,  # Éviter le filtrage des doublons
                errback=self.handle_error  # Gérer les erreurs
            )

    def handle_error(self, failure):
        """Gestionnaire d'erreurs pour diagnostiquer les problèmes"""
        self.logger.error(f"Request failed: {failure}")
        if hasattr(failure.value, 'response'):
            self.logger.error(f"Response status: {failure.value.response.status}")

    def parse(self, response):
        # Log pour debug
        self.logger.info(f"Parsing response for {response.meta['numero']}")
        self.logger.info(f"Response status: {response.status}")

        # Sauvegarder la page pour inspection (optionnel, pour debug)
        if response.status == 200:
            filename = f"debug_{response.meta['numero'].replace('.', '_')}.html"
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.logger.info(f"Saved page to {filename}")

        item = KboScraperItem()
        item["enterprise_number"] = response.meta["numero"]

        # Méthodes alternatives d'extraction du statut
        status = None

        # Méthode 1 : XPath original
        status = response.xpath('//div[@id="table"]/table[1]/tbody/tr[3]/td[2]/strong/span/text()').get()

        if not status:
            # Méthode 2 : Chercher le statut avec un XPath plus large
            status = response.xpath('//td[contains(text(), "Statut")]/following-sibling::td//text()').get()

        if not status:
            # Méthode 3 : Chercher par classe ou autres attributs
            status = response.xpath('//span[@class="status"]//text()').get()

        if not status:
            # Méthode 4 : Extraction plus générale
            status_elements = response.xpath('//strong/span/text()').getall()
            if status_elements:
                # Prendre le premier élément qui pourrait être un statut
                status = status_elements[0]

        # Log des résultats
        self.logger.info(f"Status found: {status}")

        item["status"] = status.strip() if status else "Status not found"

        # Extraction d'autres informations pour vérification
        # Essayer d'extraire le nom de l'entreprise
        company_name = response.xpath('//h1//text()').get()
        if company_name:
            self.logger.info(f"Company name found: {company_name}")

        # Vérifier si la page contient le numéro d'entreprise
        if response.meta["numero"] in response.text:
            self.logger.info("Enterprise number found in page content")
        else:
            self.logger.warning("Enterprise number NOT found in page content")

        # Extraire tout le texte de la page pour inspection
        all_text = response.xpath('//text()').getall()
        if all_text:
            self.logger.info(f"Page contains {len(all_text)} text elements")

        yield item

