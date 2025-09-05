#!/usr/bin/env python3
"""
Script pour exécuter les spiders en leur fournissant les numéros d'entreprise depuis MongoDB
Usage:
  python run_spiders.py --spider kbo_spider --limit 10
  python run_spiders.py --spider ejustice_spider --limit 5
  python run_spiders.py --spider consult_spider --limit 20
  python run_spiders.py --spider all --limit 10
  python run_spiders.py --spider kbo_spider --diagnose
"""
import argparse
import pymongo
import subprocess
import sys
import os
import time
from typing import List, Optional


class SpiderRunner:
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017", mongo_db: str = "kbo_db"):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    def test_mongodb_connection(self) -> bool:
        """Test la connexion à MongoDB"""
        try:
            client = pymongo.MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            client.server_info()  # Force une connexion
            client.close()
            print("✅ Connexion MongoDB réussie")
            return True
        except Exception as e:
            print(f"❌ Impossible de se connecter à MongoDB: {e}")
            print(f"🔍 Vérifiez que MongoDB est démarré et accessible sur {self.mongo_uri}")
            return False

    def diagnose_database(self) -> None:
        """Diagnostic de la base de données"""
        try:
            client = pymongo.MongoClient(self.mongo_uri)
            db = client[self.mongo_db]

            print(f"\n🔍 Diagnostic de la base '{self.mongo_db}':")
            print("-" * 40)

            # Lister les collections
            collections = db.list_collection_names()
            print(f"Collections trouvées: {collections}")

            if "entreprises" in collections:
                count = db.entreprises.count_documents({})
                print(f"📊 Nombre d'entreprises: {count}")

                if count > 0:
                    # Échantillon de données
                    sample = db.entreprises.find_one()
                    if sample:
                        print(f"🔍 Champs disponibles: {list(sample.keys())}")
                        if "enterprise_number" in sample:
                            print(f"📝 Exemple de numéro: {sample['enterprise_number']}")
                else:
                    print("⚠️  Collection vide")
            else:
                print("❌ Collection 'entreprises' non trouvée")

            client.close()
            print("-" * 40)

        except Exception as e:
            print(f"❌ Erreur lors du diagnostic: {e}")

    def get_enterprise_numbers(self, limit: Optional[int] = None) -> List[str]:
        """Récupère les numéros d'entreprise depuis MongoDB"""
        try:
            client = pymongo.MongoClient(self.mongo_uri)
            db = client[self.mongo_db]

            # Vérifier d'abord si la collection existe et contient des données
            if "entreprises" not in db.list_collection_names():
                print("❌ Collection 'entreprises' n'existe pas dans MongoDB")
                client.close()
                return []

            # Compter le nombre total de documents
            total_count = db.entreprises.count_documents({})
            print(f"📊 {total_count} entreprises trouvées dans la base")

            if total_count == 0:
                print("❌ Aucune entreprise trouvée dans la collection")
                client.close()
                return []

            # Récupérer les numéros d'entreprise
            query = {"enterprise_number": {"$exists": True, "$ne": None}}
            projection = {"enterprise_number": 1}

            if limit:
                cursor = db.entreprises.find(query, projection).limit(limit)
                print(f"🔍 Récupération de {limit} numéros d'entreprise maximum...")
            else:
                cursor = db.entreprises.find(query, projection)
                print(f"🔍 Récupération de tous les numéros d'entreprise...")

            enterprise_numbers = []
            for doc in cursor:
                if "enterprise_number" in doc and doc["enterprise_number"]:
                    enterprise_numbers.append(doc["enterprise_number"])

            client.close()

            print(f"✅ {len(enterprise_numbers)} numéros d'entreprise valides récupérés depuis MongoDB")

            if len(enterprise_numbers) == 0:
                print("⚠️  Aucun numéro d'entreprise valide trouvé")
                print("💡 Vérifiez que le champ 'enterprise_number' existe et n'est pas vide")

            return enterprise_numbers

        except Exception as e:
            print(f"❌ Erreur lors de la récupération des données MongoDB: {e}")
            print(f"🔍 Détails: {str(e)}")
            return []

    def run_spider(self, spider_name: str, enterprise_numbers: List[str]) -> bool:
        """Exécute un spider avec les numéros d'entreprise fournis"""
        if not enterprise_numbers:
            print(f"⚠️  Aucun numéro d'entreprise à traiter pour {spider_name}")
            return False

        # Joindre les numéros avec des virgules
        numbers_str = ",".join(enterprise_numbers)

        # Commande Scrapy
        cmd = [
            "scrapy", "crawl", spider_name,
            "-a", f"enterprise_numbers={numbers_str}"
        ]

        print(f"🚀 Lancement de {spider_name} avec {len(enterprise_numbers)} numéros...")
        print(f"Commande: {' '.join(cmd)}")

        try:
            # Exécuter le spider
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())

            if result.returncode == 0:
                print(f"✅ {spider_name} terminé avec succès")
                return True
            else:
                print(f"❌ {spider_name} a échoué:")
                print(f"STDOUT: {result.stdout}")
                print(f"STDERR: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Erreur lors de l'exécution de {spider_name}: {e}")
            return False

    def run_kbo_spider_with_csv(self, limit: Optional[int] = None) -> bool:
        """Exécute le spider KBO (qui utilise déjà un CSV)"""
        cmd = ["scrapy", "crawl", "kbo_spider"]
        if limit:
            print(f"⚠️  Note: kbo_spider utilise son propre fichier CSV, le paramètre limit est ignoré")

        print(f"🚀 Lancement de kbo_spider...")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())

            if result.returncode == 0:
                print(f"✅ kbo_spider terminé avec succès")
                return True
            else:
                print(f"❌ kbo_spider a échoué:")
                print(f"STDOUT: {result.stdout}")
                print(f"STDERR: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Erreur lors de l'exécution de kbo_spider: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="Exécuteur de spiders KBO")
    parser.add_argument("--spider", choices=["kbo_spider", "ejustice_spider", "consult_spider", "all"],
                        required=True, help="Spider à exécuter")
    parser.add_argument("--limit", type=int, help="Nombre maximum d'entreprises à traiter")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="URI MongoDB")
    parser.add_argument("--mongo-db", default="kbo_db", help="Base de données MongoDB")
    parser.add_argument("--diagnose", action="store_true", help="Effectuer un diagnostic de la base de données")

    args = parser.parse_args()

    runner = SpiderRunner(args.mongo_uri, args.mongo_db)

    # Test de la connexion MongoDB
    if not runner.test_mongodb_connection():
        sys.exit(1)

    # Diagnostic si demandé
    if args.diagnose:
        runner.diagnose_database()
        return

    if args.spider == "kbo_spider":
        # KBO spider utilise son propre CSV
        success = runner.run_kbo_spider_with_csv(args.limit)
        sys.exit(0 if success else 1)

    elif args.spider == "all":
        # Phase 1: Exécuter KBO spider d'abord (pour remplir la DB)
        print("\n" + "=" * 50)
        print("Phase 1: KBO Spider - Récupération des données de base")
        print("=" * 50)
        kbo_success = runner.run_kbo_spider_with_csv()

        if not kbo_success:
            print("❌ Échec du spider KBO - Arrêt de l'exécution")
            sys.exit(1)

        print("✅ KBO Spider terminé - Attente de 5 secondes pour la synchronisation...")
        time.sleep(5)

        # Phase 2: Récupérer les numéros d'entreprise APRÈS l'exécution de KBO
        print("\n📋 Récupération des numéros d'entreprise depuis la base mise à jour...")
        enterprise_numbers = runner.get_enterprise_numbers(args.limit)

        if not enterprise_numbers:
            print("❌ Aucun numéro d'entreprise trouvé après l'exécution de KBO")
            print("💡 Vérifiez que le spider KBO a bien inséré des données")
            sys.exit(1)

        # Phase 3: Exécuter les autres spiders avec les numéros récupérés
        spiders_to_run = ["ejustice_spider", "consult_spider"]
        all_success = True

        for spider in spiders_to_run:
            print("\n" + "=" * 50)
            print(f"Phase: {spider} - Traitement de {len(enterprise_numbers)} entreprises")
            print("=" * 50)
            success = runner.run_spider(spider, enterprise_numbers)
            all_success = all_success and success

            if success:
                print(f"✅ {spider} terminé avec succès")
                # Petite pause entre les spiders
                if spider != spiders_to_run[-1]:  # Pas de pause après le dernier
                    print("⏳ Pause de 3 secondes avant le spider suivant...")
                    time.sleep(3)
            else:
                print(f"⚠️  {spider} a échoué mais continue avec les autres...")

        print("\n" + "=" * 60)
        print(f"🏁 Exécution terminée - Succès global: {'✅' if all_success else '⚠️'}")
        print("=" * 60)
        sys.exit(0 if all_success else 1)

    else:
        # Spider individuel (ejustice ou consult)
        enterprise_numbers = runner.get_enterprise_numbers(args.limit)

        if not enterprise_numbers:
            print("❌ Impossible de récupérer les numéros d'entreprise")
            sys.exit(1)

        success = runner.run_spider(args.spider, enterprise_numbers)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()