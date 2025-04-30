from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta

# =============================================
# Paramètres du DAG
# =============================================

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
}

dag = DAG(
    dag_id='bottleneck_pipeline',
    default_args=default_args,
    description='Pipeline démonstrateur BottleNeck - version Airflow',
    schedule_interval='0 9 15 * *',  # Tous les 15 du mois à 9h
    start_date=days_ago(1),
    catchup=False,
    tags=['bottleneck', 'pipeline', 'airflow'],
)

# =============================================
# Fonctions Python placeholder (exemple)
# =============================================

def dummy_task(task_name):
    print(f"Tâche {task_name} exécutée.")

# =============================================
# Définition des tâches principales
# =============================================

# 📦 Téléchargement des données
telechargement_donnees = BashOperator(
    task_id='telechargement_donnees',
    bash_command='python /opt/airflow/dags/scripts/00_download_and_extract.py',
    dag=dag
)

# 📄 Conversion Excel -> CSV
conversion_excel_csv = BashOperator(
    task_id='conversion_excel_csv',
    bash_command='python /opt/airflow/dags/scripts/01_excel_to_csv.py',
    dag=dag
)

# ☁️ Upload CSV bruts
upload_csv_bruts = BashOperator(
    task_id='upload_csv_bruts',
    bash_command='python /opt/airflow/dags/scripts/02_upload_to_minio.py',
    dag=dag
)

# ✅ Vérification de l'upload
verification_upload = BashOperator(
    task_id='verification_upload',
    bash_command='python /opt/airflow/dags/scripts/03_verify_upload.py',
    dag=dag
)

# 🧹 Nettoyage des données
nettoyage_donnees = BashOperator(
    task_id='nettoyage_donnees',
    bash_command='python /opt/airflow/dags/scripts/05_clean_data.py',
    dag=dag
)

# 📅 Upload des fichiers nettoyés
upload_clean = BashOperator(
    task_id='upload_clean',
    bash_command='python /opt/airflow/dags/scripts/06_upload_clean_to_minio.py',
    dag=dag
)

# 🗒️ Dédoublonnage
with TaskGroup('dedoublonnage_group', tooltip="Dédoublonnage + tests") as dedoublonnage_group:
    dedoublonnage = BashOperator(
        task_id='dedoublonnage',
        bash_command='python /opt/airflow/dags/scripts/08_dedoublonnage.py',
        dag=dag
    )
    tests_dedoublonnage = BashOperator(
        task_id='tests_dedoublonnage',
        bash_command='python /opt/airflow/dags/scripts/tests/test_08_dedoublonnage.py',
        dag=dag
    )
    dedoublonnage >> tests_dedoublonnage

# 🔗 Fusion
with TaskGroup('fusion_group', tooltip="Fusion + tests") as fusion_group:
    fusion = BashOperator(
        task_id='fusion',
        bash_command='python /opt/airflow/dags/scripts/09_fusion.py',
        dag=dag
    )
    tests_fusion = BashOperator(
        task_id='tests_fusion',
        bash_command='python /opt/airflow/dags/scripts/tests/test_09_fusion.py',
        dag=dag
    )
    fusion >> tests_fusion

# 💾 Snapshot de la base
snapshot_base = BashOperator(
    task_id='snapshot_base',
    bash_command='python /opt/airflow/dags/scripts/10_create_snapshot.py',
    dag=dag
)

# ✨ Calcul CA & Z-score parallèle
with TaskGroup('calculs_parallel', tooltip="CA et Z-score") as calculs_parallel:
    with TaskGroup('ca_group', tooltip="Chiffre d'affaires") as ca_group:
        calcul_ca = BashOperator(
            task_id='calcul_ca',
            bash_command='python /opt/airflow/dags/scripts/11_calcul_ca.py',
            dag=dag
        )
        test_ca = BashOperator(
            task_id='test_ca',
            bash_command='python /opt/airflow/dags/scripts/tests/test_11_validate_ca.py',
            dag=dag
        )
        calcul_ca >> test_ca

    with TaskGroup('zscore_group', tooltip="Z-score") as zscore_group:
        calcul_zscore = BashOperator(
            task_id='calcul_zscore',
            bash_command='python /opt/airflow/dags/scripts/12_calcul_zscore_upload.py',
            dag=dag
        )
        test_zscore = BashOperator(
            task_id='test_zscore',
            bash_command='python /opt/airflow/dags/scripts/tests/test_12_validate_zscore.py',
            dag=dag
        )
        calcul_zscore >> test_zscore

# 📈 Rapport final
rapport_final = BashOperator(
    task_id='rapport_final',
    bash_command='python /opt/airflow/dags/scripts/13_generate_final_report.py',
    dag=dag
)

# ☁️ Upload des logs
upload_logs_final = BashOperator(
    task_id='upload_logs_final',
    bash_command='python /opt/airflow/dags/scripts/14_upload_all_logs.py',
    trigger_rule=TriggerRule.ALL_DONE,  # Toujours exécuter
    dag=dag
)

# =============================================
# Orchestration des dépendances
# =============================================

(
    telechargement_donnees >>
    conversion_excel_csv >>
    upload_csv_bruts >>
    verification_upload >>
    nettoyage_donnees >>
    upload_clean >>
    dedoublonnage_group >>
    fusion_group >>
    snapshot_base >>
    calculs_parallel >>
    rapport_final >>
    upload_logs_final
)
