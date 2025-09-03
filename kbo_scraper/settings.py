BOT_NAME = "kbo_scraper"

SPIDER_MODULES = ["kbo_scraper.spiders"]
NEWSPIDER_MODULE = "kbo_scraper.spiders"

ROBOTSTXT_OBEY = False

DEFAULT_REQUEST_HEADERS = {
    "Accept-Language": "fr"  # ⚠️ pour forcer version FR
}

# 🆕 Pipelines mis à jour avec support publications
ITEM_PIPELINES = {
    "kbo_scraper.pipelines.ValidationPipeline": 200,
    "kbo_scraper.pipelines.PublicationDeduplicationPipeline": 250,
    "kbo_scraper.pipelines.MongoPipeline": 300,
}

# Configuration MongoDB
MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "kbo_db"

# 🆕 Configuration spécifique pour ejustice
EJUSTICE_SETTINGS = {
    'CONCURRENT_REQUESTS': 1,
    'DOWNLOAD_DELAY': 3,
    'RANDOMIZE_DOWNLOAD_DELAY': 1.0,
    'RETRY_TIMES': 3,
    'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
}

# Logging
LOG_LEVEL = 'INFO'
LOG_FILE = 'scrapy.log'

# Cache pour éviter de re-scraper les mêmes URLs
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600  # 1 heure
HTTPCACHE_DIR = 'httpcache'

# AutoThrottle pour s'adapter automatiquement
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
AUTOTHROTTLE_DEBUG = False

# User-Agent rotation (optionnel)
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'kbo_scraper.middlewares.RotateUserAgentMiddleware': 400,
}

# Liste d'User-Agents pour la rotation
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
]