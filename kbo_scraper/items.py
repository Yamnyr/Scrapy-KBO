# items.py
import scrapy


class KboScraperItem(scrapy.Item):
    # Informations de base
    enterprise_number = scrapy.Field()
    status = scrapy.Field()
    juridical_situation = scrapy.Field()
    start_date = scrapy.Field()

    # Informations sur l'entreprise
    company_name = scrapy.Field()
    abbreviation = scrapy.Field()
    headquarters_address = scrapy.Field()

    # Coordonnées
    phone = scrapy.Field()
    email = scrapy.Field()
    website = scrapy.Field()

    # Informations juridiques
    entity_type = scrapy.Field()
    legal_form = scrapy.Field()
    establishment_units = scrapy.Field()

    # Qualités et activités
    qualities = scrapy.Field()
    tva_activity_2025 = scrapy.Field()
    onss_activity_2025 = scrapy.Field()

    functions = scrapy.Field()
    functions_json = scrapy.Field()

    qualities_json = scrapy.Field()

    nace_codes = scrapy.Field()

    external_links = scrapy.Field()
    entity_links = scrapy.Field()
    financial_data = scrapy.Field()