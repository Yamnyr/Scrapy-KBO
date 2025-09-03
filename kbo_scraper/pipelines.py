import pymongo
import json
from datetime import datetime
from itemadapter import ItemAdapter


class MongoPipeline:
    collection_name = "entreprises"
    publications_collection_name = "moniteur_publications"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "kbo_db")
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Traitement différent selon le type de spider
        if spider.name == "ejustice_spider":
            return self.process_publication_item(adapter, spider)
        else:
            return self.process_enterprise_item(adapter, spider)

    def process_enterprise_item(self, adapter, spider):
        """Traite les items d'entreprise du spider KBO"""
        self.db[self.collection_name].update_one(
            {"enterprise_number": adapter["enterprise_number"]},
            {"$set": dict(adapter)},
            upsert=True
        )
        return adapter.item

    def process_publication_item(self, adapter, spider):
        """Traite les items de publications du spider ejustice"""
        enterprise_number = adapter["enterprise_number"]

        if "moniteur_publications" in adapter:
            # Décoder les publications JSON
            try:
                publications_data = json.loads(adapter["moniteur_publications"])

                # Ajouter la date de scraping
                for pub in publications_data:
                    pub["scraping_date"] = datetime.now().isoformat()

                # Mettre à jour l'entreprise avec les nouvelles publications
                self.db[self.collection_name].update_one(
                    {"enterprise_number": enterprise_number},
                    {
                        "$addToSet": {
                            "moniteur_publications": {"$each": publications_data}
                        },
                        "$set": {"moniteur_last_updated": datetime.now()}
                    },
                    upsert=True
                )

                # Sauvegarder aussi dans une collection séparée pour les publications
                for pub in publications_data:
                    pub_doc = {
                        **pub,
                        "_id": f"{enterprise_number}_{pub.get('publication_number', 'unknown')}_{pub.get('publication_date', 'nodate')}"
                    }

                    self.db[self.publications_collection_name].update_one(
                        {"_id": pub_doc["_id"]},
                        {"$set": pub_doc},
                        upsert=True
                    )

                spider.logger.info(f"Publications sauvegardées pour {enterprise_number}: {len(publications_data)}")

            except json.JSONDecodeError as e:
                spider.logger.error(f"Erreur décodage JSON publications: {e}")

        return adapter.item


class PublicationDeduplicationPipeline:
    """Pipeline pour dédupliquer les publications identiques"""

    def __init__(self):
        self.seen_publications = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if spider.name == "ejustice_spider" and "moniteur_publications" in adapter:
            try:
                publications_data = json.loads(adapter["moniteur_publications"])
                unique_publications = []

                for pub in publications_data:
                    # Créer une clé unique pour la publication
                    pub_key = (
                        pub.get("enterprise_number", ""),
                        pub.get("publication_number", ""),
                        pub.get("publication_date", ""),
                        (pub.get("title") or "")[:50]
                    )

                    if pub_key not in self.seen_publications:
                        self.seen_publications.add(pub_key)
                        unique_publications.append(pub)
                    else:
                        spider.logger.info(f"Publication dupliquée ignorée: {pub_key}")

                # Mettre à jour l'item avec les publications uniques
                adapter["moniteur_publications"] = json.dumps(unique_publications, ensure_ascii=False)

            except json.JSONDecodeError:
                pass

        return adapter.item


class ValidationPipeline:
    """Pipeline pour valider les données avant sauvegarde"""

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Validation commune
        if not adapter.get("enterprise_number"):
            spider.logger.error("Item rejeté: pas de numéro d'entreprise")
            raise DropItem("Numéro d'entreprise manquant")

        # Validation spécifique aux publications
        if spider.name == "ejustice_spider":
            if "moniteur_publications" in adapter:
                try:
                    publications = json.loads(adapter["moniteur_publications"])
                    if not publications:
                        spider.logger.info("Aucune publication valide trouvée")
                        raise DropItem("Aucune publication valide")

                    # Validation de chaque publication
                    valid_publications = []
                    for pub in publications:
                        if self.validate_publication(pub, spider):
                            valid_publications.append(pub)

                    if not valid_publications:
                        raise DropItem("Aucune publication valide après validation")

                    adapter["moniteur_publications"] = json.dumps(valid_publications, ensure_ascii=False)

                except json.JSONDecodeError:
                    raise DropItem("Données de publication invalides")

        return adapter.item

    def validate_publication(self, pub, spider):
        title = pub.get("title") or ""
        number = pub.get("publication_number") or ""

        if not title and not number:
            spider.logger.warning("Publication rejetée: ni titre ni numéro")
            return False

        # Vérif de la date
        date_str = pub.get("publication_date") or ""
        if date_str and not re.search(r'\d{4}', date_str):
            spider.logger.warning(f"Date suspecte: {date_str}")

        return True
