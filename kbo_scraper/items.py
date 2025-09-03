import scrapy

class KboScraperItem(scrapy.Item):
    enterprise_number = scrapy.Field()
    status = scrapy.Field()  # nouveau champ pour le statut
