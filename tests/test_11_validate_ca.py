# === Script 11_test - Validation du calcul du chiffre d'affaires ===
# Ce script valide que les tables `ca_par_produit` et `ca_total` sont cohérentes
# après leur création et upload. Il vérifie : total attendu, absence de NULLs, 
# nombre de lignes et absence de valeurs négatives.

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

LOG_FILE = LOGS_PATH / "test_11_validate_ca.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale
# ==============================================================================
def main():
    try:
        con = duckdb.connect("data/bottleneck.duckdb")
        logger.success("✅ Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Échec de connexion à DuckDB : {e}")
        sys.exit(1)

    try:
        # Vérification CA total
        ca_total = con.execute("SELECT ca_total FROM ca_total").fetchone()[0]
        assert round(ca_total, 2) == 387837.60, f"❌ Montant CA incorrect : {ca_total} € (attendu : 387 837.60 €)"
        logger.success(f"💰 CA total validé : {ca_total:,.2f} € ✅")

        # Nombre de lignes
        nb_produits = con.execute("SELECT COUNT(*) FROM ca_par_produit").fetchone()[0]
        assert nb_produits == 573, f"❌ Nombre de lignes incorrect : {nb_produits} (attendu : 573)"
        logger.success(f"📦 Nombre de produits : {nb_produits} lignes")

        # Vérification des nulls
        nulls = con.execute("""
            SELECT COUNT(*) FROM ca_par_produit
            WHERE product_id IS NULL OR post_title IS NULL 
              OR price IS NULL OR stock_quantity IS NULL 
              OR chiffre_affaires IS NULL
        """).fetchone()[0]
        assert nulls == 0, f"❌ Valeurs nulles détectées : {nulls} lignes"
        logger.success("✅ Aucune valeur nulle détectée dans ca_par_produit")

        # Vérification CA négatif
        negatives = con.execute("""
            SELECT COUNT(*) FROM ca_par_produit WHERE chiffre_affaires < 0
        """).fetchone()[0]
        assert negatives == 0, f"❌ CA négatif détecté : {negatives} lignes"
        logger.success("✅ Aucun CA négatif détecté")

        logger.success("🎯 Tous les tests de validation du CA sont passés avec succès.")

    except Exception as e:
        logger.error(f"❌ Erreur dans les tests de CA : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    main()
