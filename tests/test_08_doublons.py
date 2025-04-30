# === Script de test 08b - Vérification de l'absence de doublons après dédoublonnage ===
# Ce script vérifie qu'aucun doublon n'existe dans les tables erp_dedup, web_dedup, liaison_dedup
# en se basant sur les clés primaires attendues : product_id, sku.

import os
import sys
import duckdb
from pathlib import Path
from loguru import logger
import warnings

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔧 Configuration des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "test_08_doublons.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📋 Fonction principale : validation de l'absence de doublons
# ==============================================================================
def main():
    try:
        con = duckdb.connect("data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion échouée : {e}")
        sys.exit(1)

    try:
        erp_dup = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM erp_dedup
        """).fetchone()[0]

        web_dup = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT sku) FROM web_dedup
        """).fetchone()[0]

        liaison_dup = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM liaison_dedup
        """).fetchone()[0]

        assert erp_dup == 0, f"❌ Doublons détectés dans erp_dedup : {erp_dup}"
        assert web_dup == 0, f"❌ Doublons détectés dans web_dedup : {web_dup}"
        assert liaison_dup == 0, f"❌ Doublons détectés dans liaison_dedup : {liaison_dup}"

        logger.success("✅ Aucun doublon détecté dans les tables dédoublonnées.")
        logger.success("🎯 Test d'unicité post-dédoublonnage validé.")

    except Exception as e:
        logger.error(f"❌ Erreur lors de la vérification des doublons : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    main()
