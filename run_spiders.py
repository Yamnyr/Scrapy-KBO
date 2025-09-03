#!/usr/bin/env python3
"""
Script pour exécuter les spiders KBO et ejustice
Usage: python run_spiders.py [kbo|ejustice|both] [--limit N]
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
        """Vérifie que MongoDB est accessible"""
        try:
            # Utiliser server_info() au lieu de admin.command()
            self.mongo_client.server_info()
            print("✅ MongoDB connecté")
            return True
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}")
            print("💡 Vérifiez que MongoDB est démarré sur localhost:27017")
            return False

    def run_kbo_spider(self, limit=None):
        """Exécute le spider KBO"""
        print(f"\n🕷️  Démarrage du spider KBO...")

        cmd = ["scrapy", "crawl", "kbo_spider"]
        if limit:
            cmd.extend(["-s", f"CLOSESPIDER_ITEMCOUNT={limit}"])

        start_time = time.time()

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")

            if result.returncode == 0:
                elapsed = time.time() - start_time
                print(f"✅ Spider KBO terminé en {elapsed:.1f}s")
                print(f"Output: {result.stdout[-200:]}")  # Derniers 200 caractères
                return True
            else:
                print(f"❌ Erreur spider KBO: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Exception spider KBO: {e}")
            return False

    def run_ejustice_spider(self, limit=None):
        """Exécute le spider ejustice"""
        print(f"\n🕷️  Démarrage du spider ejustice...")

        # Vérifier qu'il y a des entreprises en base
        count = self.db.entreprises.count_documents({})
        if count == 0:
            print("⚠️  Aucune entreprise en base, exécutez d'abord le spider KBO")
            return False

        print(f"📊 {count} entreprises trouvées en base")

        cmd = ["scrapy", "crawl", "ejustice_spider"]
        if limit:
            cmd.extend(["-s", f"CLOSESPIDER_ITEMCOUNT={limit}"])

        start_time = time.time()

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")

            if result.returncode == 0:
                elapsed = time.time() - start_time
                print(f"✅ Spider ejustice terminé en {elapsed:.1f}s")
                print(f"Output: {result.stdout[-200:]}")
                return True
            else:
                print(f"❌ Erreur spider ejustice: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Exception spider ejustice: {e}")
            return False

    def get_stats(self):
        """Affiche les statistiques de la base"""
        print(f"\n📊 STATISTIQUES")
        print("=" * 50)

        # Stats entreprises
        entreprises_count = self.db.entreprises.count_documents({})
        print(f"🏢 Entreprises: {entreprises_count}")

        # Stats publications
        with_publications = self.db.entreprises.count_documents({"moniteur_publications": {"$exists": True, "$ne": []}})
        print(f"📰 Entreprises avec publications: {with_publications}")

        # Total publications
        pipeline = [
            {"$match": {"moniteur_publications": {"$exists": True}}},
            {"$project": {"pub_count": {"$size": "$moniteur_publications"}}},
            {"$group": {"_id": None, "total": {"$sum": "$pub_count"}}}
        ]

        result = list(self.db.entreprises.aggregate(pipeline))
        total_publications = result[0]["total"] if result else 0
        print(f"📄 Total publications: {total_publications}")

        # Publications collection séparée
        pub_collection_count = self.db.moniteur_publications.count_documents({})
        print(f"📋 Publications (collection séparée): {pub_collection_count}")

        # Dernière mise à jour
        last_update = self.db.entreprises.find_one(
            {"moniteur_last_updated": {"$exists": True}},
            sort=[("moniteur_last_updated", -1)]
        )

        if last_update:
            last_date = last_update.get("moniteur_last_updated")
            print(f"🕐 Dernière mise à jour: {last_date}")

        print("=" * 50)

    def clean_database(self):
        """Nettoie la base de données"""
        print("\n🧹 Nettoyage de la base...")

        confirm = input("Voulez-vous vraiment supprimer toutes les données? (y/N): ")
        if confirm.lower() == 'y':
            self.db.entreprises.delete_many({})
            self.db.moniteur_publications.delete_many({})
            print("✅ Base nettoyée")
        else:
            print("❌ Nettoyage annulé")


def main():
    parser = argparse.ArgumentParser(description="Exécute les spiders KBO et ejustice")
    parser.add_argument(
        'spider',
        choices=['kbo', 'ejustice', 'both'],
        help='Spider à exécuter'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='Limite le nombre d\'items traités'
    )
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Nettoie la base avant de commencer'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Affiche seulement les statistiques'
    )

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

    if args.spider in ['kbo', 'both']:
        success &= runner.run_kbo_spider(args.limit)

    if args.spider in ['ejustice', 'both']:
        # Attendre un peu entre les spiders
        if args.spider == 'both':
            print("\n⏳ Pause de 5s entre les spiders...")
            time.sleep(5)

        success &= runner.run_ejustice_spider(args.limit)

    # Afficher les stats finales
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