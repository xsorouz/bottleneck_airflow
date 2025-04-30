# === Script de test 05 - Validation du nettoyage des données ===
# Ce script vérifie que les tables nettoyées existent, sont peuplées,
# et respectent les exigences minimales avant le dédoublonnage.

import os
import duckdb
from pathlib import Path
from loguru import logger
import sys
import warnings

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔧 Configuration des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "test_clean_data.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📋 Fonction principale : validation des données nettoyées
# ==============================================================================
def main():
    try:
        con = duckdb.connect("data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB réussie.")
    except Exception as e:
        logger.error(f"❌ Échec de connexion à DuckDB : {e}")
        sys.exit(1)

    try:
        nb_erp = con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0]
        nb_web = con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0]
        nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]

        assert nb_erp > 0, "Table erp_clean vide"
        assert nb_web > 0, "Table web_clean vide"
        assert nb_liaison > 0, "Table liaison_clean vide"

        logger.success(f"✅ erp_clean contient {nb_erp} lignes.")
        logger.success(f"✅ web_clean contient {nb_web} lignes.")
        logger.success(f"✅ liaison_clean contient {nb_liaison} lignes.")
        logger.success("🎯 Les données nettoyées sont valides.")

    except Exception as e:
        logger.error(f"❌ Erreur de validation : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    main()
