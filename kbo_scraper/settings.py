BOT_NAME = "kbo_scraper"

SPIDER_MODULES = ["kbo_scraper.spiders"]
NEWSPIDER_MODULE = "kbo_scraper.spiders"

ROBOTSTXT_OBEY = False

DEFAULT_REQUEST_HEADERS = {
    "Accept-Language": "fr"  # ⚠️ pour forcer version FR
}

ITEM_PIPELINES = {
    "kbo_scraper.pipelines.MongoPipeline": 300,
}

MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "kbo_db"
