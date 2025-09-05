# items.py - Version mise à jour
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

    # 🆕 Champs pour les publications Moniteur Belge
    moniteur_publications = scrapy.Field()
    moniteur_last_updated = scrapy.Field()


    entrepreneurial_capacities = scrapy.Field()
    entrepreneurial_capacities_json = scrapy.Field()

    authorizations = scrapy.Field()
    belac_details = scrapy.Field()


    deposits = scrapy.Field()  # <- nouveau champ
    url = scrapy.Field()          # <- ajouter ce champ
    data = scrapy.Field()         # <- ajouter ce champ
class MoniteurPublicationItem(scrapy.Item):
    """Item spécifique pour une publication du Moniteur Belge"""
    enterprise_number = scrapy.Field()
    publication_number = scrapy.Field()
    title = scrapy.Field()
    code = scrapy.Field()
    address = scrapy.Field()
    publication_type = scrapy.Field()
    publication_date = scrapy.Field()
    reference = scrapy.Field()
    image_url = scrapy.Field()
    detail_url = scrapy.Field()
    full_content = scrapy.Field()
    scraping_date = scrapy.Field()

class ConsultItem(scrapy.Item):
    """Item spécifique pour les données financières de consult.cbso"""
    enterprise_number = scrapy.Field()
    balance_sheet = scrapy.Field()
    income_statement = scrapy.Field()
    social_balance = scrapy.Field()
    financial_ratios = scrapy.Field()
    scraping_date = scrapy.Field()
    last_update = scrapy.Field()
    deposits = scrapy.Field()
    url = scrapy.Field()
    data = scrapy.Field()