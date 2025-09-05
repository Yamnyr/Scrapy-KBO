#!/usr/bin/env python3
"""
Script pour exécuter les spiders KBO, ejustice, consult et new_spider
Usage: python run_spiders.py [kbo|ejustice|consult|new|both|all] [--limit N]
"""

import subprocess
import sys
import time
import argparse
import pymongo
from datetime import datetime

class SpiderRunner:
    def __init__(self):
        self.mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
        self.db = self.mongo_client["kbo_db"]

    def check_mongodb(self):
        try:
            self.mongo_client.server_info()
            print("✅ MongoDB connecté")
            return True
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}")
            print("💡 Vérifiez que MongoDB est démarré sur localhost:27017")
            return False

    # --- KBO spider ---
    def run_kbo_spider(self, limit=None):
        print(f"\n🕷️  Démarrage du spider KBO...")
        cmd = ["scrapy", "crawl", "kbo_spider"]
        if limit:
            cmd.extend(["-s", f"CLOSESPIDER_ITEMCOUNT={limit}"])
        return self._run_subprocess(cmd, "KBO")

    # --- ejustice spider ---
    def run_ejustice_spider(self, limit=None):
        print(f"\n🕷️  Démarrage du spider ejustice...")
        if self.db.entreprises.count_documents({}) == 0:
            print("⚠️  Aucune entreprise en base, exécutez d'abord le spider KBO")
            return False
        cmd = ["scrapy", "crawl", "ejustice_spider"]
        if limit:
            cmd.extend(["-s", f"CLOSESPIDER_ITEMCOUNT={limit}"])
        return self._run_subprocess(cmd, "ejustice")

    # --- consult spider ---
    def run_consult_spider(self, limit=None):
        print(f"\n🕷️  Démarrage du spider consult...")
        if self.db.entreprises.count_documents({}) == 0:
            print("⚠️  Aucune entreprise en base, exécutez d'abord le spider KBO")
            return False
        cmd = ["scrapy", "crawl", "consult_spider"]
        if limit:
            cmd.extend(["-s", f"CLOSESPIDER_ITEMCOUNT={limit}"])
        return self._run_subprocess(cmd, "consult")

    # --- nouveau spider ---
    def run_new_spider(self, limit=None):
        print(f"\n🕷️  Démarrage du nouveau spider...")
        cmd = ["scrapy", "crawl", "new_spider"]
        if limit:
            cmd.extend(["-s", f"CLOSESPIDER_ITEMCOUNT={limit}"])
        return self._run_subprocess(cmd, "new_spider")

    def _run_subprocess(self, cmd, name):
        start_time = time.time()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")
            elapsed = time.time() - start_time
            if result.returncode == 0:
                print(f"✅ Spider {name} terminé en {elapsed:.1f}s")
                print(f"Output: {result.stdout[-200:]}")
                return True
            else:
                print(f"❌ Erreur spider {name}: {result.stderr}")
                return False
        except Exception as e:
            print(f"❌ Exception spider {name}: {e}")
            return False

    # --- stats, nettoyage, etc. ---
    def get_stats(self):
        print(f"\n📊 STATISTIQUES")
        print("=" * 50)
        entreprises_count = self.db.entreprises.count_documents({})
        print(f"🏢 Entreprises: {entreprises_count}")
        with_publications = self.db.entreprises.count_documents({"moniteur_publications": {"$exists": True, "$ne": []}})
        print(f"📰 Entreprises avec publications: {with_publications}")
        with_financial = self.db.entreprises.count_documents({"financial_data": {"$exists": True, "$ne": {}}})
        print(f"💰 Entreprises avec données financières: {with_financial}")
        pipeline = [
            {"$match": {"moniteur_publications": {"$exists": True}}},
            {"$project": {"pub_count": {"$size": "$moniteur_publications"}}},
            {"$group": {"_id": None, "total": {"$sum": "$pub_count"}}}
        ]
        result = list(self.db.entreprises.aggregate(pipeline))
        total_publications = result[0]["total"] if result else 0
        print(f"📄 Total publications: {total_publications}")
        pub_collection_count = self.db.moniteur_publications.count_documents({})
        print(f"📋 Publications (collection séparée): {pub_collection_count}")
        last_update = self.db.entreprises.find_one({"moniteur_last_updated": {"$exists": True}}, sort=[("moniteur_last_updated", -1)])
        if last_update:
            print(f"🕐 Dernière mise à jour: {last_update.get('moniteur_last_updated')}")
        print("=" * 50)

    def clean_database(self):
        print("\n🧹 Nettoyage de la base...")
        confirm = input("Voulez-vous vraiment supprimer toutes les données? (y/N): ")
        if confirm.lower() == 'y':
            self.db.entreprises.delete_many({})
            self.db.moniteur_publications.delete_many({})
            print("✅ Base nettoyée")
        else:
            print("❌ Nettoyage annulé")

def main():
    parser = argparse.ArgumentParser(description="Exécute les spiders KBO, ejustice, consult et new_spider")
    parser.add_argument('spider', choices=['kbo', 'ejustice', 'consult', 'new', 'both', 'all'], help='Spider à exécuter')
    parser.add_argument('--limit', '-l', type=int, help='Limite le nombre d\'items traités')
    parser.add_argument('--clean', action='store_true', help='Nettoie la base avant de commencer')
    parser.add_argument('--stats', action='store_true', help='Affiche seulement les statistiques')
    args = parser.parse_args()

    runner = SpiderRunner()
    if not runner.check_mongodb():
        sys.exit(1)

    if args.stats:
        runner.get_stats()
        return

    if args.clean:
        runner.clean_database()

    success = True

    if args.spider in ['kbo', 'both', 'all']:
        success &= runner.run_kbo_spider(args.limit)

    if args.spider in ['ejustice', 'both', 'all']:
        time.sleep(5)
        success &= runner.run_ejustice_spider(args.limit)

    if args.spider in ['consult', 'all']:
        time.sleep(5)
        success &= runner.run_consult_spider(args.limit)

    if args.spider in ['new', 'all']:
        time.sleep(5)
        success &= runner.run_new_spider(args.limit)

    print("\n" + "=" * 50)
    runner.get_stats()

    if success:
        print("\n✅ Tous les spiders terminés avec succès!")
        sys.exit(0)
    else:
        print("\n❌ Erreurs détectées lors de l'exécution")
        sys.exit(1)

if __name__ == "__main__":
    main()
