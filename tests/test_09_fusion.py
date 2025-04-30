# === Script de test 09 - Validation de la fusion des données ===
# Ce script vérifie que la table 'fusion' existe, contient bien 714 lignes,
# et que toutes les colonnes critiques attendues sont bien présentes.

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

LOG_FILE = LOGS_PATH / "test_09_fusion.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📋 Fonction principale de test de la table fusion
# ==============================================================================
def main():
    try:
        con = duckdb.connect("data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Connexion échouée : {e}")
        sys.exit(1)

    try:
        nb_rows = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]
        assert nb_rows == 714, f"❌ Table fusion contient {nb_rows} lignes (attendu : 714)"
        logger.success(f"✅ Table fusion : {nb_rows} lignes (attendu : 714)")

        columns = con.execute("PRAGMA table_info('fusion')").fetchdf()["name"].tolist()
        for col in ["product_id", "price", "stock_status", "post_title"]:
            assert col in columns, f"❌ Colonne manquante : {col}"
        logger.success("✅ Toutes les colonnes critiques sont présentes.")

        logger.success("🎯 Validation complète de la table fusion réussie.")

    except Exception as e:
        logger.error(f"❌ Échec du test de fusion : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    main()
