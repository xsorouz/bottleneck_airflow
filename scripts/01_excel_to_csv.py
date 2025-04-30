# === Script 01 - Conversion Excel ➔ CSV (robuste et compatible Airflow) ===
# Ce script convertit les fichiers Excel extraits en CSV dans 'data/inputs/'.
# Il applique un nettoyage minimal et journalise chaque étape.

import os
import sys
import warnings
from pathlib import Path
import pandas as pd
from loguru import logger

# ==============================================================================
# 🔧 Initialisation des chemins et logs
# ==============================================================================
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")  # Défaut local
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

INPUTS_PATH = Path("data/inputs")   # Répertoire des fichiers Excel
CSV_OUTPUT_PATH = Path("data/inputs")  # Répertoire de sortie des CSV
CSV_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "conversion_excel_csv.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📄 Mapping : fichiers Excel ➔ CSV
# ==============================================================================
files_mapping = {
    "Fichier_erp.xlsx": "erp.csv",
    "Fichier_web.xlsx": "web.csv",
    "fichier_liaison.xlsx": "liaison.csv",
}

# ==============================================================================
# 🧽 Fonction : nettoyage minimal des DataFrames
# ==============================================================================
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all", axis=0)  # Supprime lignes vides
    df = df.dropna(how="all", axis=1)  # Supprime colonnes vides
    return df

# ==============================================================================
# 🚀 Point d'entrée principal
# ==============================================================================
def main():
    logger.info("🔄 Début de la conversion des fichiers Excel en CSV...")

    for excel_file, csv_file in files_mapping.items():
        excel_path = INPUTS_PATH / excel_file
        csv_path = CSV_OUTPUT_PATH / csv_file

        if not excel_path.exists():
            logger.error(f"❌ Fichier Excel introuvable : {excel_file}")
            sys.exit(1)

        try:
            df = pd.read_excel(excel_path)
            df = clean_dataframe(df)
            df.to_csv(csv_path, index=False)

            if not csv_path.exists():
                raise FileNotFoundError(f"CSV non généré : {csv_file}")
            if df.empty:
                raise ValueError(f"DataFrame vide : {csv_file}")

            logger.success(f"✅ {excel_file} ➔ {csv_file} ({len(df)} lignes)")

        except Exception as e:
            logger.error(f"❌ Erreur pour {excel_file} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers Excel ont été convertis avec succès.")

# ==============================================================================
# 📌 Lancement direct
# ==============================================================================
if __name__ == "__main__":
    main()
