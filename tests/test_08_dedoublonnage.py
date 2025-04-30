# === Script de test 08 - Validation des tables dédoublonnées ===
# Ce script contrôle que les tables erp_dedup, web_dedup et liaison_dedup
# existent bien dans DuckDB et contiennent un nombre de lignes > 0.

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

LOG_FILE = LOGS_PATH / "test_08_dedoublonnage.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📋 Fonction principale : test des tables dédoublonnées
# ==============================================================================
def main():
    try:
        con = duckdb.connect("data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion échouée : {e}")
        sys.exit(1)

    try:
        nb_erp = con.execute("SELECT COUNT(*) FROM erp_dedup").fetchone()[0]
        nb_web = con.execute("SELECT COUNT(*) FROM web_dedup").fetchone()[0]
        nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_dedup").fetchone()[0]

        assert nb_erp > 0, "❌ Table erp_dedup vide"
        assert nb_web > 0, "❌ Table web_dedup vide"
        assert nb_liaison > 0, "❌ Table liaison_dedup vide"

        logger.success(f"✅ erp_dedup : {nb_erp} lignes")
        logger.success(f"✅ web_dedup : {nb_web} lignes")
        logger.success(f"✅ liaison_dedup : {nb_liaison} lignes")
        logger.success("🎯 Test de création des tables dédoublonnées validé.")

    except Exception as e:
        logger.error(f"❌ Erreur dans le test de dédoublonnage : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    main()
