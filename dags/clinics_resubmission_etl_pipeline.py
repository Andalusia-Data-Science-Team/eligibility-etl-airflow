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
from src.resubmission import transform_loop

# Configure debug logger
logger = logging.getLogger("debug_logs")
logger.setLevel(logging.DEBUG)

with open("passcode.json", "r") as file:
    db_configs = json.load(file)
db_configs = db_configs["DB_NAMES"]

with open(Path("sql") / "resubmission.sql", "r") as file:
    query = file.read()

CAIRO_TZ = pendulum.timezone("Africa/Cairo")

# Enhanced default args with anonymous SMTP configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False,  # Set to True if you want success emails for individual tasks
    "email": email_list,  # Default email list
    "on_failure_callback": failure_callback,
}


@dag(
    dag_id="clinics_resubmission_job",
    default_args=default_args,
    start_date=pendulum.now(CAIRO_TZ).subtract(days=1),
    schedule_interval="30 7 * * *",
    catchup=False,
    tags=["resubmission", "SNB", "AKW", "ALW", "MKR", "LCH"],
    max_active_runs=2,  # Prevent overlapping runs
    description="ETL pipeline for Outpatient Clinics Claims Resubmission Copilot",
    # DAG-level email configuration
    params={
        "email_on_dag_failure": True,
        "notification_emails": email_list,
    },
)
def clinics_resubmission_etl_pipeline():
    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def extract(clinic_name, clinic_passcode):
        df = read_data(query, clinic_passcode, logger)
        if df.empty:
            raise AirflowSkipException("No data was found, quitting resubmission job")

        df = df.drop_duplicates(keep="last")
        df = df.loc[df["VisitClassificationEnName"] != "Ambulatory"]
        timestamp = datetime.now().strftime("%Y%m%d")
        temp = f"/tmp/extracted_resubmission_{clinic_name}_{timestamp}.parquet"
        df.to_parquet(temp)
        return temp

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def transform(clinic_name, extracted_data):
        extracted_data = pd.read_parquet(extracted_data)
        result_df = transform_loop(extracted_data, logger)
        result_df["BU"] = clinic_name
        timestamp = datetime.now().strftime("%Y%m%d")
        result_file = f"/tmp/result_resubmission_{clinic_name}_{timestamp}.parquet"
        result_df.to_parquet(result_file)

        return result_file

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def load(result_file):
        result_df = pd.read_parquet(result_file)
        update_table(
            db_configs["BI"], "Clinics_Resubmission_Copilot", result_df, logger
        )

    @task
    def cleanup_files(extracted_data, transformed_data):
        """Clean up the extracted data file"""
        try:
            if extracted_data and os.path.exists(extracted_data):
                os.remove(extracted_data)
                logger.info(f"Cleaned up extraction file: {extracted_data}")

            if transformed_data and os.path.exists(transformed_data):
                os.remove(transformed_data)
                logger.info(f"Cleaned up transformed file: {transformed_data}")

        except Exception as e:
            logger.debug(f"Error cleaning up file: {str(e)}")
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


dag = clinics_resubmission_etl_pipeline()
