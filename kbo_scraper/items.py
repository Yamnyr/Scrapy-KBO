import scrapy

class KboScraperItem(scrapy.Item):
    enterprise_number = scrapy.Field()
    status = scrapy.Field()
    company_name = scrapy.Field()  # Ajout du nom
    juridical_form = scrapy.Field()  # Ajout de la forme juridique
    start_date = scrapy.Field()  # Ajout de la date de début