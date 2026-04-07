import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.etl_utils import email_list, START_DATE, failure_callback, read_data, update_table_EGY
from src.predictions_EGY import make_preds

# Configure debug logger
logger = logging.getLogger("debug_logs")
logger.setLevel(logging.DEBUG)

with open("passcode.json", "r") as file:
    db_configs = json.load(file)
db_configs = db_configs["DB_NAMES"]

with open(Path("sql") / "prediction_EGY.sql", "r") as file:
    query = file.read()


def resolve_service_conflicts(df):
    """
    Governance step:
    - Identifies duplicates by VisitID + Service_Name
    - Applies the 'last' prediction and reason to all rows in that group
    - Keeps ALL rows so the report has no blanks
    """
    df = df.sort_values(by=["VisitID", "Service_Name", "VisitServiceID"])
    df["Medical_Prediction"] = df.groupby(["VisitID", "Service_Name"])["Medical_Prediction"].transform("last")
    df["Reason/Recommendation"] = df.groupby(["VisitID", "Service_Name"])["Reason/Recommendation"].transform("last")
    return df


# Enhanced default args with anonymous SMTP configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False,
    "email": email_list,
    "on_failure_callback": failure_callback,
}


@dag(
    dag_id="predictions_EGY",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=timedelta(minutes=35),
    catchup=False,
    tags=["predictions"],
    max_active_runs=10,
    description="ETL pipeline for Egypt Medical Predictions",
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
    def extract(clinic_name, clinic_passcode, **context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")

        df = read_data(query, clinic_passcode, logger)
        if df.empty:
            raise AirflowSkipException("Query returned 0 rows, quitting predictions job")

        # Filter by BU column
        df = df[df["BU"] == clinic_name]

        if df.empty:
            raise AirflowSkipException(f"No rows for BU={clinic_name}, skipping")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_file = f"/tmp/{run_id}_extracted_claims_{clinic_name}_{timestamp}.parquet"
        df.to_parquet(temp_file)
        return temp_file

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def transform(clinic_name, extracted_data, **context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")

        df = pd.read_parquet(extracted_data)

        total_records = len(df)
        BATCH_SIZE = 1000
        total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE

        # Log existing batch files for this run only
        try:
            existing_batch_files = sorted(
                f for f in os.listdir("/tmp") if f.startswith(f"{run_id}_history_batch_{clinic_name}_")
            )
            logger.info(f"Found {len(existing_batch_files)} existing batch files in /tmp before transform")
            if existing_batch_files:
                logger.info("Existing batch files:\n" + "\n".join(existing_batch_files))
        except Exception as e:
            logger.warning(f"Could not list /tmp directory: {e}")

        logger.info(
            f"Starting transform for {clinic_name}: {total_records} records "
            f"→ {total_batches} batches (batch size = {BATCH_SIZE})"
        )

        batch_files = []
        completed_batches = 0

        for batch_idx, start in enumerate(range(0, total_records, BATCH_SIZE), start=1):
            end = min(start + BATCH_SIZE, total_records)
            batch_file = f"/tmp/{run_id}_history_batch_{clinic_name}_{start}.parquet"

            logger.info(f"[{clinic_name} Batch {batch_idx}/{total_batches}] Range {start} → {end}")

            if os.path.exists(batch_file):
                logger.info(
                    f"[{clinic_name} Batch {batch_idx}/{total_batches}] "
                    f"SKIPPED – already processed ({batch_file})"
                )
                batch_files.append(batch_file)
                completed_batches += 1
                continue

            logger.info(f"[{clinic_name} Batch {batch_idx}/{total_batches}] RUNNING predictions")

            preds, metrics = make_preds(df.iloc[start:end])
            logger.info("========== LLM COST REPORT ==========")
            logger.info(f"Total input tokens:  {metrics['total_input_tokens']}")
            logger.info(f"Total output tokens: {metrics['total_output_tokens']}")
            logger.info(f"Total tokens:        {metrics['total_tokens']}")
            logger.info(f"Total cost:         ${metrics['total_cost']:.4f}")
            logger.info("====================================")

            preds["BU"] = clinic_name
            preds.to_parquet(batch_file)

            logger.info(
                f"[{clinic_name} Batch {batch_idx}/{total_batches}] "
                f"COMPLETED and saved to {batch_file}"
            )

            batch_files.append(batch_file)
            completed_batches += 1

        logger.info(
            f"Transform finished for {clinic_name}: {completed_batches}/{total_batches} batches processed"
        )

        return batch_files

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def clean_conflicts(clinic_name, history_files, **context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")

        dfs = []
        for file in history_files:
            if os.path.exists(file):
                dfs.append(pd.read_parquet(file))
            else:
                logger.warning(f"Missing batch file: {file}")

        if not dfs:
            raise ValueError(f"No batch files found for conflict resolution for {clinic_name}")

        df = pd.concat(dfs, ignore_index=True)

        before = len(df)
        df_clean = resolve_service_conflicts(df)
        after = len(df_clean)

        logger.info(f"[{clinic_name}] Governance cleanup: removed {before - after} duplicated service records")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cleaned_file = f"/tmp/{run_id}_history_cleaned_{clinic_name}_{timestamp}.parquet"
        df_clean.to_parquet(cleaned_file)

        return cleaned_file

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def load(clinic_name, cleaned_file):
        logger.info(f"START LOAD TASK for {clinic_name}")

        df = pd.read_parquet(cleaned_file)

        # Deduplicate by primary key
        before_count = len(df)
        df = df.drop_duplicates(subset=['VisitServiceID'], keep='last')
        after_count = len(df)
        logger.info(f"[{clinic_name}] Deduplication: Reduced {before_count} rows to {after_count} unique VisitServiceIDs")

        df['VisitServiceID'] = df['VisitServiceID'].astype(int)

        pred_df = df[["VisitServiceID", "Medical_Prediction", "Reason/Recommendation", "BU"]]

        BATCH_SIZE = 1000
        total_records = len(pred_df)
        total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info(f"[{clinic_name}] STARTING LOAD | {total_records} records → {total_batches} batches")

        for batch_idx, start in enumerate(range(0, total_records, BATCH_SIZE), start=1):
            end = min(start + BATCH_SIZE, total_records)
            batch_df = pred_df.iloc[start:end]

            logger.info(f"[{clinic_name} BI Batch {batch_idx}/{total_batches}] Processing Rows {start} → {end}")

            update_table_EGY(
                db_configs["BI"],
                "EGY_MedPred_Final",
                batch_df,
                logger,
            )

        logger.info(f"[{clinic_name}] BI LOAD COMPLETED SUCCESSFULLY")
        logger.info(f"[{clinic_name}] LOAD TASK FINISHED SUCCESSFULLY")

    @task
    def cleanup_files(extracted_data, batch_files, cleaned_file):
        """Clean up all tmp files for this run"""
        if not batch_files:
            batch_files = []

        files_to_remove = [extracted_data, cleaned_file] + batch_files

        for file_path in files_to_remove:
            try:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")

    # DAG flow
    BU = ["AMH", "ASH", "SMH"]

    for unit in BU:
        extracted = extract.override(task_id=f"{unit}_extract")(unit, db_configs["BI"])
        batch_files = transform.override(task_id=f"{unit}_transform")(unit, extracted)
        cleaned = clean_conflicts.override(task_id=f"{unit}_clean_conflicts")(unit, batch_files)
        loaded = load.override(task_id=f"{unit}_load")(unit, cleaned)
        loaded >> cleanup_files.override(task_id=f"{unit}_cleanup")(
            extracted, batch_files, cleaned
        )


dag = predictions_etl_pipeline()