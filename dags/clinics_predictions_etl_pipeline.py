import json
import logging
import os
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.etl_utils import email_list, failure_callback, read_data, update_table
from src.predictions import make_preds

# Configure debug logger
logger = logging.getLogger("debug_logs")
logger.setLevel(logging.DEBUG)

with open("passcode.json", "r") as file:
    db_configs = json.load(file)
db_configs = db_configs["DB_NAMES"]

with open(Path("sql") / "claims_prediction.sql", "r") as file:
    query = file.read()

CAIRO_TZ = pendulum.timezone("Africa/Cairo")

# Enhanced default args with anonymous SMTP configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False,  # Set to True if you want success emails for individual tasks
    "email": email_list,
    "on_failure_callback": failure_callback,
}


@dag(
    dag_id="clinics_predictions_job",
    default_args=default_args,
    start_date=pendulum.now(CAIRO_TZ).subtract(days=1),
    schedule_interval="0 23,4,8,12,16,20 * * *",  # Every 4 hours
    catchup=False,
    tags=["predictions", "SNB", "AKW", "ALW", "MKR", "LCH"],
    max_active_runs=10,  # Prevent overlapping runs
    description="ETL pipeline for Outpatient Clinics Claims Medical Predictions",
    # DAG-level email configuration
    params={
        "email_on_dag_failure": True,
        "notification_emails": email_list,
    },
)
def predictions_etl_pipeline():
    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def extract(clinic_name, clinic_passcode):
        df = read_data(query, clinic_passcode, logger)
        if df.empty:
            raise AirflowSkipException(
                "Query returned 0 rows, quitting predictions job"
            )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_file = f"/tmp/extracted_claims_{clinic_name}_{timestamp}.parquet"
        df.to_parquet(temp_file)
        return temp_file

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def transform(clinic_name, extracted_data):
        extracted_data = pd.read_parquet(extracted_data)
        history_df = make_preds(extracted_data)
        history_df["BU"] = clinic_name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        history_file = f"/tmp/history_{clinic_name}_{timestamp}.parquet"
        history_df.to_parquet(history_file)
        return history_file

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def load(history):
        history = pd.read_parquet(history)
        pred_df = history[
            ["VisitServiceID", "Medical_Prediction", "Reason/Recommendation", "BU"]
        ]
        update_table(db_configs["BI"], "Clinics_Predictions_DotCare", pred_df, logger)
        update_table(
            db_configs["AI"], "Clinics_Claims_Predictions_History", history, logger
        )

    @task
    def cleanup_files(extracted_data, transformed_data):
        """Clean up the extracted data file"""
        try:
            if extracted_data and os.path.exists(extracted_data):
                os.remove(extracted_data)
                print(f"Cleaned up extraction file: {extracted_data}")

            if transformed_data and os.path.exists(transformed_data):
                os.remove(transformed_data)
                print(f"Cleaned up extraction file: {transformed_data}")

        except Exception as e:
            print(f"Error cleaning up extraction file: {str(e)}")
            # Don't raise here as cleanup failure shouldn't fail the DAG

    # DAG flow
    BU = ["SNB", "AKW", "ALW", "MKR", "LCH"]
    for unit in BU:  # Override Airflow generic task IDs to distinguish between BUs
        extracted = extract.override(task_id=f"{unit}_extract")(unit, db_configs[unit])
        transformed = transform.override(task_id=f"{unit}_transform")(unit, extracted)
        loaded = load.override(task_id=f"{unit}_load")(transformed)
        loaded >> cleanup_files.override(task_id=f"{unit}_cleanup")(
            extracted, transformed
        )


dag = predictions_etl_pipeline()
