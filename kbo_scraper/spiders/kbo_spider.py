import scrapy
import pandas as pd
from kbo_scraper.items import KboScraperItem
import logging
import re
import json
import copy


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
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
    }

    def start_requests(self):
        df = pd.read_csv("enterprise.csv")
        sample_df = df.sample(n=10, random_state=42)

        for numero in sample_df["EnterpriseNumber"]:
            numero_clean = numero.replace(".", "")
            url = f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?lang=fr&ondernemingsnummer={numero_clean}"

            self.logger.info(f"Requesting URL: {url}")

            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={"numero": numero},
                dont_filter=True,
                errback=self.handle_error
            )

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure}")
        if hasattr(failure.value, 'response'):
            self.logger.error(f"Response status: {failure.value.response.status}")

    def clean_text(self, text):
        if text:
            return re.sub(r'\s+', ' ', text.strip())
        return None

    # ============================
    # EXTRACTION FUNCTIONS
    # ============================

    def extract_nace_codes(self, response, version):
        nace_data = []
        section_rows = response.xpath(
            f'//h2[contains(text(), "Code Nacebel version {version}")]/ancestor::tr/following-sibling::tr'
        )
        for row in section_rows:
            if row.xpath('.//h2'):
                break
            texts = row.xpath('.//text()').getall()
            texts = [t.strip() for t in texts if t.strip()]
            if not texts:
                continue
            full_text = " ".join(texts)
            match = re.match(r'(TVA|ONSS)\s*' + version + r'\s*([0-9.]+)\s*-\s*(.+)', full_text)
            if match:
                nace_type = match.group(1)
                code = match.group(2)
                desc_date = match.group(3)
                date_match = re.search(r'Depuis le (.+)$', desc_date)
                if date_match:
                    date = date_match.group(1).strip()
                    description = re.sub(r'\s*Depuis le .+$', '', desc_date).strip()
                else:
                    date = "Date not found"
                    description = desc_date.strip()
                nace_data.append({
                    "version": version,
                    "type": nace_type,
                    "code": code,
                    "description": description,
                    "date": date
                })
        return nace_data

    def extract_qualities_from_page(self, response):
        qualities_data = []
        qualities_rows = response.xpath(
            '//h2[contains(text(), "Qualités")]/ancestor::tr/following-sibling::tr'
        )
        for row in qualities_rows:
            if row.xpath('.//h2[contains(text(), "Autorisations")]'):
                break
            quality_texts = row.xpath('.//text()').getall()
            if not quality_texts:
                continue
            quality_text = ' '.join([t.strip() for t in quality_texts if t.strip()])
            if not quality_text:
                continue
            date_match = re.search(r'Depuis le (.+?)$', quality_text)
            if date_match:
                date = date_match.group(1).strip()
                quality_name = re.sub(r'\s*Depuis le .+$', '', quality_text).strip()
            else:
                date = "Date not found"
                quality_name = quality_text.strip()
            if quality_name:
                qualities_data.append({
                    'name': self.clean_text(quality_name),
                    'date': self.clean_text(date)
                })
        return qualities_data

    def extract_functions_from_page(self, response):
        functions_data = []
        hidden_functions = response.xpath('//table[@id="toonfctie"]//tr')
        for row in hidden_functions:
            function_role = row.xpath('.//td[1]//text()').get()
            function_name = row.xpath('.//td[2]//text()').getall()
            function_date = row.xpath('.//td[3]//span[@class="upd"]/text()').get()
            if function_role and function_name:
                name_clean = ' '.join([n.strip() for n in function_name if n.strip()])
                name_clean = re.sub(r'\s*,\s*', ', ', name_clean)
                name_clean = re.sub(r'\s+', ' ', name_clean).strip()
                functions_data.append({
                    'role': self.clean_text(function_role),
                    'name': name_clean,
                    'date': self.clean_text(function_date) if function_date else "Date not found"
                })
        return functions_data

    def extract_financial_data(self, response):
        financial_data = {}
        capital = response.xpath(
            '//h2[contains(text(), "Données financières")]/ancestor::tr/following-sibling::tr[td[contains(text(), "Capital")]]/td[2]//text()'
        ).get()
        financial_data["capital"] = self.clean_text(capital) if capital else "Not found"
        ag = response.xpath(
            '//h2[contains(text(), "Données financières")]/ancestor::tr/following-sibling::tr[td[contains(text(), "Assemblée générale")]]/td[2]//text()'
        ).get()
        financial_data["general_assembly"] = self.clean_text(ag) if ag else "Not found"
        end_date = response.xpath(
            '//h2[contains(text(), "Données financières")]/ancestor::tr/following-sibling::tr[td[contains(text(), "Date de fin de l\'année comptable")]]/td[2]//text()'
        ).get()
        financial_data["fiscal_year_end"] = self.clean_text(end_date) if end_date else "Not found"
        return financial_data

    def extract_entity_links(self, response):
        links_section = response.xpath(
            '//h2[contains(text(), "Liens entre entités")]/ancestor::tr/following-sibling::tr[1]//text()'
        ).getall()
        links_section = [t.strip() for t in links_section if t.strip()]
        return " ".join(links_section) if links_section else "Not found"

    def extract_external_links(self, response):
        external_links = []
        links = response.xpath('//h2[contains(text(), "Liens externes")]/ancestor::tr/following-sibling::tr[1]//a')
        for link in links:
            href = link.xpath('./@href').get()
            label = link.xpath('.//text()').get()
            if href and label:
                external_links.append({
                    "label": self.clean_text(label),
                    "url": href
                })
        return external_links

    def extract_entrepreneurial_capacities(self, response):
        capacities_data = []
        capacity_rows = response.xpath(
            '//h2[contains(text(), "Capacités entrepreneuriales")]/ancestor::tr/following-sibling::tr'
        )
        for row in capacity_rows:
            if row.xpath('.//h2'):
                break
            texts = row.xpath('.//text()').getall()
            if not texts:
                continue
            capacity_text = ' '.join([t.strip() for t in texts if t.strip()])
            if not capacity_text:
                continue
            date_match = re.search(r'Depuis le (.+?)$', capacity_text)
            if date_match:
                date = date_match.group(1).strip()
                capacity_name = re.sub(r'\s*Depuis le .+$', '', capacity_text).strip()
            else:
                date = "Date not found"
                capacity_name = capacity_text.strip()
            if capacity_name:
                capacities_data.append({
                    "name": self.clean_text(capacity_name),
                    "date": self.clean_text(date)
                })
        return capacities_data

    def extract_authorizations(self, response):
        authorizations = []
        rows = response.xpath('//h2[contains(text(), "Autorisations")]/ancestor::tr/following-sibling::tr')
        for row in rows:
            links = row.xpath('.//a[@class="external"]')
            for link in links:
                href = link.xpath('./@href').get()
                label = link.xpath('normalize-space(string(.))').get()
                if href:
                    authorizations.append({
                        "label": self.clean_text(label),
                        "url": href
                    })
        return authorizations

    # ============================
    # MAIN PARSE
    # ============================

    def parse(self, response):
        numero = response.meta['numero']
        item = KboScraperItem()
        item["enterprise_number"] = numero

        # ========= INFORMATIONS =========
        status = response.xpath('//td[contains(text(), "Statut:")]/following-sibling::td//span/text()').get()
        item["status"] = self.clean_text(status) if status else "Status not found"
        juridical_situation = response.xpath(
            '//td[contains(text(), "Situation juridique:")]/following-sibling::td//span[@class="pageactief"]/text()'
        ).get()
        item["juridical_situation"] = self.clean_text(juridical_situation) if juridical_situation else "Not found"
        start_date = response.xpath('//td[contains(text(), "Date de début:")]/following-sibling::td/text()').get()
        item["start_date"] = self.clean_text(start_date) if start_date else "Not found"
        company_name_elements = response.xpath(
            '//td[contains(text(), "Dénomination:")]/following-sibling::td//text()'
        ).getall()
        item["company_name"] = company_name_elements[0].strip() if company_name_elements else "Name not found"
        abbreviation_elements = response.xpath(
            '//td[contains(text(), "Abréviation:")]/following-sibling::td//text()'
        ).getall()
        item["abbreviation"] = abbreviation_elements[0].strip() if abbreviation_elements else "Not found"
        address_elements = response.xpath(
            '//td[contains(text(), "Adresse du siège:")]/following-sibling::td//text()'
        ).getall()
        if address_elements:
            full_address = ' '.join(
                [elem.strip() for elem in address_elements if elem.strip() and "Depuis le" not in elem])
            item["headquarters_address"] = self.clean_text(full_address)
        else:
            item["headquarters_address"] = "Not found"
        phone = response.xpath('//td[contains(text(), "Numéro de téléphone:")]/following-sibling::td/text()').get()
        item["phone"] = self.clean_text(phone) if phone else "Not found"
        email = response.xpath('//td[contains(text(), "E-mail:")]/following-sibling::td/text()').get()
        item["email"] = self.clean_text(email) if email else "Not found"
        website = response.xpath('//td[contains(text(), "Adresse web:")]/following-sibling::td/text()').get()
        item["website"] = self.clean_text(website) if website else "Not found"
        entity_type = response.xpath('//td[contains(text(), "Type d\'entité:")]/following-sibling::td/text()').get()
        item["entity_type"] = self.clean_text(entity_type) if entity_type else "Not found"
        legal_form_elements = response.xpath(
            '//td[contains(text(), "Forme légale:")]/following-sibling::td//text()'
        ).getall()
        item["legal_form"] = legal_form_elements[0].strip() if legal_form_elements else "Not found"
        establishment_units = response.xpath(
            '//td[contains(text(), "Nombre d\'unités d\'établissement")]/following-sibling::td/strong/text()'
        ).get()
        item["establishment_units"] = self.clean_text(establishment_units) if establishment_units else "Not found"

        # ========= QUALITÉS =========
        qualities_data = self.extract_qualities_from_page(response)
        if qualities_data:
            qualities_formatted = [f"{q['name']} ({q['date']})" for q in qualities_data]
            item["qualities"] = "; ".join(qualities_formatted)
            item["qualities_json"] = json.dumps(qualities_data, ensure_ascii=False)
        else:
            item["qualities"] = "Not found"
            item["qualities_json"] = "[]"

        # ========= FONCTIONS =========
        functions_data = self.extract_functions_from_page(response)
        if functions_data:
            functions_formatted = [f"{f['role']}: {f['name']} ({f['date']})" for f in functions_data]
            item["functions"] = "; ".join(functions_formatted)
            item["functions_json"] = json.dumps(functions_data, ensure_ascii=False)
        else:
            item["functions"] = "Not found"
            item["functions_json"] = "[]"

        # ========= NACE =========
        nace_all = []
        nace_all.extend(self.extract_nace_codes(response, "2025"))
        nace_all.extend(self.extract_nace_codes(response, "2008"))
        nace_all.extend(self.extract_nace_codes(response, "2003"))
        item["nace_codes"] = json.dumps(nace_all, ensure_ascii=False) if nace_all else "[]"

        # ========= DONNÉES FINANCIÈRES =========
        financial_data = self.extract_financial_data(response)
        item["financial_data"] = json.dumps(financial_data, ensure_ascii=False)

        # ========= LIENS ENTRE ENTITÉS =========
        item["entity_links"] = self.extract_entity_links(response)

        # ========= LIENS EXTERNES =========
        external_links = self.extract_external_links(response)
        item["external_links"] = json.dumps(external_links, ensure_ascii=False) if external_links else "[]"

        # ========= CAPACITÉS ENTREPRENEURIALES =========
        capacities_data = self.extract_entrepreneurial_capacities(response)
        if capacities_data:
            capacities_formatted = [f"{c['name']} ({c['date']})" for c in capacities_data]
            item["entrepreneurial_capacities"] = "; ".join(capacities_formatted)
            item["entrepreneurial_capacities_json"] = json.dumps(capacities_data, ensure_ascii=False)
        else:
            item["entrepreneurial_capacities"] = "Not found"
            item["entrepreneurial_capacities_json"] = "[]"

        # ========= AUTORISATIONS =========
        authorizations = self.extract_authorizations(response)
        item["authorizations"] = json.dumps(authorizations, ensure_ascii=False)

        yield item