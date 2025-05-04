# === DAG BottleNeck Pipeline - version test allégée ===
# Ce DAG exécute uniquement le script 00 de téléchargement et extraction
# pour valider l’intégration avec Airflow (via BashOperator).
# Les autres étapes sont temporairement commentées pour test progressif.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum

# =============================================
# Paramètres du DAG
# =============================================
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
}

with DAG(
    dag_id='bottleneck_pipeline',
    default_args=default_args,
    description='Pipeline démonstrateur BottleNeck - version test uniquement avec étape 00',
    schedule=None,  # ❌ Pas de déclenchement automatique
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['bottleneck', 'pipeline', 'test'],
) as dag:

    # ✅ Étape 00 - Téléchargement et extraction
    telechargement_donnees = BashOperator(
        task_id='telechargement_donnees',
        bash_command='python /opt/airflow/scripts/00_download_and_extract.py || exit 1',
        do_xcom_push=False,
    )

    # =============================================
    # ⏸️ Étapes suivantes (désactivées temporairement)
    # =============================================
    # conversion_excel_csv = BashOperator(
    #     task_id='conversion_excel_csv',
    #     bash_command='python /opt/airflow/scripts/01_excel_to_csv.py',
    # )
    #
    # upload_csv_bruts = BashOperator(
    #     task_id='upload_csv_bruts',
    #     bash_command='python /opt/airflow/scripts/02_upload_to_minio.py',
    # )
    #
    # etc...

    # =============================================
    # Orchestration minimale (étape 00 uniquement)
    # =============================================
    telechargement_donnees
