#!/usr/bin/env python3
"""
Script pour lancer tous les spiders KBO de manière séquentielle ou parallèle
"""

import os
import sys
import time
import argparse
import subprocess
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('run_spiders.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SpiderRunner:
    """Classe pour gérer l'exécution des spiders"""

    def __init__(self, limit=10, mode='sequential'):
        self.limit = limit
        self.mode = mode
        self.spiders = [
            {
                'name': 'kbo_spider',
                'description': 'Spider principal pour extraire les données KBO',
                'priority': 1,
                'command': ['scrapy', 'crawl', 'kbo_spider']
            },
            {
                'name': 'ejustice_spider',
                'description': 'Spider pour les publications du Moniteur Belge',
                'priority': 2,
                'command': ['scrapy', 'crawl', 'ejustice_spider', '-a', f'limit={limit}']
            },
            {
                'name': 'consult_spider',
                'description': 'Spider pour les données financières CBSO',
                'priority': 3,
                'command': ['scrapy', 'crawl', 'consult_spider', '-a', f'limit={limit}']
            }
        ]
        self.results = {}

    def check_mongodb_connection(self):
        """Vérifier la connexion MongoDB"""
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
            client.server_info()
            client.close()
            logger.info("Connexion MongoDB OK")
            return True
        except Exception as e:
            logger.error(f"Erreur connexion MongoDB: {e}")
            return False

    def check_dependencies(self):
        """Vérifier que Scrapy et les dépendances sont installés"""
        try:
            result = subprocess.run(['scrapy', 'version'],
                                    capture_output=True, text=True, check=True)
            logger.info(f"Scrapy version: {result.stdout.strip()}")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("Scrapy n'est pas installé ou accessible")
            return False

    def get_enterprise_count(self):
        """Récupérer le nombre d'entreprises en base"""
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017")
            db = client["kbo_db"]
            count = db.entreprises.count_documents({})
            client.close()
            return count
        except Exception as e:
            logger.warning(f"Impossible de compter les entreprises: {e}")
            return 0

    def run_spider(self, spider):
        """Exécuter un spider individuel"""
        spider_name = spider['name']
        logger.info(f"Démarrage de {spider_name}: {spider['description']}")

        start_time = time.time()

        try:
            # Changer vers le répertoire du projet
            os.chdir(os.path.dirname(os.path.abspath(__file__)))

            # Exécuter le spider
            result = subprocess.run(
                spider['command'],
                capture_output=True,
                text=True,
                timeout=1800  # 30 minutes max par spider
            )

            duration = time.time() - start_time

            if result.returncode == 0:
                logger.info(f"{spider_name} terminé avec succès en {duration:.1f}s")
                status = 'SUCCESS'
                error_msg = None
            else:
                logger.error(f"{spider_name} échoué (code {result.returncode})")
                logger.error(f"STDERR: {result.stderr}")
                status = 'FAILED'
                error_msg = result.stderr

        except subprocess.TimeoutExpired:
            logger.error(f"{spider_name} interrompu (timeout 30min)")
            status = 'TIMEOUT'
            error_msg = "Timeout après 30 minutes"
            duration = 1800

        except Exception as e:
            logger.error(f"Erreur inattendue pour {spider_name}: {e}")
            status = 'ERROR'
            error_msg = str(e)
            duration = time.time() - start_time

        return {
            'spider': spider_name,
            'status': status,
            'duration': duration,
            'error': error_msg,
            'stdout': getattr(result, 'stdout', ''),
            'stderr': getattr(result, 'stderr', '')
        }

    def run_sequential(self):
        """Exécuter les spiders de manière séquentielle"""
        logger.info("Mode séquentiel: exécution des spiders un par un")

        # Trier par priorité
        sorted_spiders = sorted(self.spiders, key=lambda x: x['priority'])

        for spider in sorted_spiders:
            result = self.run_spider(spider)
            self.results[spider['name']] = result

            # Petite pause entre les spiders
            if spider != sorted_spiders[-1]:  # Pas de pause après le dernier
                logger.info("Pause de 10 secondes avant le prochain spider...")
                time.sleep(10)

    def run_parallel(self, max_workers=2):
        """Exécuter les spiders en parallèle (attention aux ressources)"""
        logger.info(f"Mode parallèle: exécution avec {max_workers} workers")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Soumettre tous les spiders
            future_to_spider = {
                executor.submit(self.run_spider, spider): spider
                for spider in self.spiders
            }

            # Récupérer les résultats au fur et à mesure
            for future in as_completed(future_to_spider):
                spider = future_to_spider[future]
                try:
                    result = future.result()
                    self.results[spider['name']] = result
                except Exception as e:
                    logger.error(f"Erreur avec {spider['name']}: {e}")
                    self.results[spider['name']] = {
                        'spider': spider['name'],
                        'status': 'ERROR',
                        'duration': 0,
                        'error': str(e)
                    }

    def generate_report(self):
        """Générer un rapport de l'exécution"""
        logger.info("\n" + "=" * 60)
        logger.info("RAPPORT D'EXÉCUTION")
        logger.info("=" * 60)

        total_duration = 0
        success_count = 0

        for spider_name, result in self.results.items():
            logger.info(f"{spider_name}: {result['status']} ({result['duration']:.1f}s)")

            if result['error']:
                logger.info(f"   Erreur: {result['error']}")

            total_duration += result['duration']
            if result['status'] == 'SUCCESS':
                success_count += 1

        logger.info(f"\nRésumé:")
        logger.info(f"   • Spiders exécutés: {len(self.results)}")
        logger.info(f"   • Succès: {success_count}/{len(self.results)}")
        logger.info(f"   • Durée totale: {total_duration:.1f}s")
        logger.info(f"   • Mode: {self.mode}")

        # Stats MongoDB
        enterprise_count = self.get_enterprise_count()
        if enterprise_count > 0:
            logger.info(f"   • Entreprises en base: {enterprise_count}")

    def save_results_to_file(self):
        """Sauvegarder les résultats dans un fichier JSON"""
        import json

        report_data = {
            'timestamp': datetime.now().isoformat(),
            'mode': self.mode,
            'limit': self.limit,
            'results': self.results,
            'summary': {
                'total_spiders': len(self.results),
                'successful': sum(1 for r in self.results.values() if r['status'] == 'SUCCESS'),
                'total_duration': sum(r['duration'] for r in self.results.values())
            }
        }

        filename = f"spider_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Rapport sauvegardé: {filename}")

    def run(self):
        """Méthode principale pour lancer tous les spiders"""
        logger.info(f"Démarrage du runner KBO (limit={self.limit}, mode={self.mode})")

        # Vérifications préalables
        if not self.check_dependencies():
            return False

        if not self.check_mongodb_connection():
            return False

        start_time = time.time()

        try:
            if self.mode == 'sequential':
                self.run_sequential()
            elif self.mode == 'parallel':
                self.run_parallel(max_workers=2)
            else:
                logger.error(f"Mode inconnu: {self.mode}")
                return False

        except KeyboardInterrupt:
            logger.warning("Interruption par l'utilisateur (Ctrl+C)")
            return False

        except Exception as e:
            logger.error(f"Erreur inattendue: {e}")
            return False

        finally:
            total_time = time.time() - start_time
            logger.info(f"Temps total d'exécution: {total_time:.1f}s")
            self.generate_report()
            self.save_results_to_file()

        return True


def main():
    """Point d'entrée principal"""
    parser = argparse.ArgumentParser(description='Lanceur de spiders KBO')

    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=10,
        help='Nombre d\'entreprises à traiter (défaut: 10)'
    )

    parser.add_argument(
        '--mode', '-m',
        choices=['sequential', 'parallel'],
        default='sequential',
        help='Mode d\'exécution: sequential (défaut) ou parallel'
    )

    parser.add_argument(
        '--spider', '-s',
        choices=['kbo_spider', 'ejustice_spider', 'consult_spider'],
        help='Exécuter un seul spider spécifique'
    )

    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Mode test: affiche ce qui serait exécuté sans le faire'
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info("[TEST] MODE DRY-RUN: simulation d'exécution")
        runner = SpiderRunner(limit=args.limit, mode=args.mode)
        for spider in runner.spiders:
            logger.info(f"Commande: {' '.join(spider['command'])}")
        return

    if args.spider:
        # Exécution d'un seul spider
        logger.info(f"[SPIDER] Exécution du spider: {args.spider}")

        command = ['scrapy', 'crawl', args.spider]
        if args.spider in ['ejustice_spider', 'consult_spider']:
            command.extend(['-a', f'limit={args.limit}'])

        try:
            result = subprocess.run(command, check=True)
            logger.info(f"[OK] {args.spider} terminé avec succès")
        except subprocess.CalledProcessError as e:
            logger.error(f"[ECHEC] {args.spider} échoué: {e}")
            sys.exit(1)
    else:
        # Exécution de tous les spiders
        runner = SpiderRunner(limit=args.limit, mode=args.mode)
        success = runner.run()

        if not success:
            sys.exit(1)


if __name__ == "__main__":
    main()
