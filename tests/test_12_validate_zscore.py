# === Script 12_test - Validation des fichiers Z-score ===
# Ce script vérifie que le fichier vins_millesimes.csv a bien été généré,
# contient exactement 30 lignes, et ne comporte ni valeurs nulles ni infinis.

import os
import sys
import pandas as pd
from pathlib import Path
from loguru import logger

# ==============================================================================
# 🔧 Configuration des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "test_12_validate_zscore.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale
# ==============================================================================
def main():
    try:
        vins_millesimes_path = Path("data/outputs/vins_millesimes.csv")
        if not vins_millesimes_path.exists():
            raise FileNotFoundError("❌ Le fichier vins_millesimes.csv est introuvable.")

        df = pd.read_csv(vins_millesimes_path)
        nb_millesimes = df.shape[0]

        assert nb_millesimes == 30, f"❌ Nombre de vins millésimés incorrect : {nb_millesimes} (attendu : 30)"
        logger.success(f"🍷 Nombre de vins millésimés détectés : {nb_millesimes} ✅")

        for col in ["price", "z_score"]:
            nulls = df[col].isnull().sum()
            infs = df[col].isin([float('inf'), float('-inf')]).sum()

            assert nulls == 0, f"❌ Valeurs nulles détectées dans {col} : {nulls} lignes"
            assert infs == 0, f"❌ Valeurs infinies détectées dans {col} : {infs} lignes"

        logger.success("✅ Aucun NaN ni Inf détecté dans les colonnes price et z_score.")
        logger.success("🎯 Test de validation du fichier Z-score terminé avec succès.")

    except Exception as e:
        logger.error(f"❌ Erreur dans les tests Z-score : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    main()
