import json
import logging
import os
import sys
import glob  
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException


def resolve_service_conflicts(df):
    """
    Governance step:
    - Identifies duplicates by VisitID + Service_Name
    - Applies the 'last' prediction and reason to all rows in that group
    - Keeps ALL rows so the report has no blanks
    """
    # 1. Sort by Service ID to make sure 'last' means the most recent one
    df = df.sort_values(by=["VisitID", "Service_Name", "VisitServiceID"])

    # 2. Use 'transform' to copy the last prediction/reason to every row in the group
    df["Medical_Prediction"] = df.groupby(["VisitID", "Service_Name"])["Medical_Prediction"].transform("last")
    df["Reason/Recommendation"] = df.groupby(["VisitID", "Service_Name"])["Reason/Recommendation"].transform("last")

    # 3. We NO LONGER call drop_duplicates() here. 
    # This ensures every row from the live system stays in the dataframe.
    return df


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.etl_utils import email_list, START_DATE, failure_callback, read_data, update_table_KSA
from src.predictions import make_preds

# Configure debug logger
logger = logging.getLogger("debug_logs")
logger.setLevel(logging.DEBUG)

with open("passcode.json", "r") as file:
    db_configs = json.load(file)
db_configs = db_configs["DB_NAMES"]

with open(Path("sql") / "predictions_KSA.sql", "r") as file:
    query = file.read()

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
    dag_id="predictions_KSA",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=timedelta(minutes=35),  # Every 35 minutes
    catchup=False,
    tags=["predictions", "AHJ", "New_solution"],
    max_active_runs=10,
    description=(
        "ETL pipeline for AHJ Claims Medical Predictions "
        "from DOT-CARE with parallel processing "
        "(ONE TIME MONTHLY MANUAL RUN)"
    ),
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
    def extract(**context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")

        df = read_data(query, db_configs["BI"], logger)
        if df is None:
            logger.error("Data extraction failed: read_data returned None")
            raise Exception("Data extraction failed; see logs for details")

        if df.empty:
            raise AirflowSkipException(
                "Query returned 0 rows, quitting predictions job"
            )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_file = f"/tmp/{run_id}_extracted_claims_{timestamp}.parquet"
        df.to_parquet(temp_file)
        return temp_file

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def transform(extracted_data, **context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")

        df = pd.read_parquet(extracted_data)

        total_records = len(df)
        BATCH_SIZE = 1000
        total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE

        # Log existing batch files for this run only
        try:
            existing_batch_files = sorted(
                f for f in os.listdir("/tmp") if f.startswith(f"{run_id}_history_batch_")
            )
            logger.info(
                f"Found {len(existing_batch_files)} existing batch files in /tmp before transform"
            )
            if existing_batch_files:
                logger.info(
                    "Existing batch files:\n" + "\n".join(existing_batch_files)
                )
        except Exception as e:
            logger.warning(f"Could not list /tmp directory: {e}")

        logger.info(
            f"Starting transform: {total_records} records "
            f"→ {total_batches} batches (batch size = {BATCH_SIZE})"
        )

        batch_files = []
        completed_batches = 0

        for batch_idx, start in enumerate(
            range(0, total_records, BATCH_SIZE), start=1
        ):
            end = min(start + BATCH_SIZE, total_records)
            batch_file = f"/tmp/{run_id}_history_batch_{start}.parquet"

            logger.info(
                f"[Batch {batch_idx}/{total_batches}] Range {start} → {end}"
            )

            if os.path.exists(batch_file):
                logger.info(
                    f"[Batch {batch_idx}/{total_batches}] "
                    f"SKIPPED – already processed ({batch_file})"
                )
                batch_files.append(batch_file)
                completed_batches += 1
                continue

            logger.info(
                f"[Batch {batch_idx}/{total_batches}] RUNNING predictions"
            )

            preds, metrics = make_preds(df.iloc[start:end])
            logger.info("========== LLM COST REPORT ==========")
            logger.info(f"Total input tokens:  {metrics['total_input_tokens']}")
            logger.info(f"Total output tokens: {metrics['total_output_tokens']}")
            logger.info(f"Total tokens:        {metrics['total_tokens']}")
            logger.info(f"Total cost:         ${metrics['total_cost']:.4f}")
            logger.info("====================================")
            preds.to_parquet(batch_file)

            logger.info(
                f"[Batch {batch_idx}/{total_batches}] "
                f"COMPLETED and saved to {batch_file}"
            )

            batch_files.append(batch_file)
            completed_batches += 1

        logger.info(
            f"Transform finished: {completed_batches}/{total_batches} batches processed"
        )

        return batch_files

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def clean_conflicts(history_files, **context):
        run_id = context["run_id"].replace(":", "_").replace("+", "_")

        dfs = []
        for file in history_files:
            if os.path.exists(file):
                dfs.append(pd.read_parquet(file))
            else:
                logger.warning(f"Missing batch file: {file}")

        if not dfs:
            raise ValueError("No batch files found for conflict resolution")

        df = pd.concat(dfs, ignore_index=True)

        before = len(df)
        df_clean = resolve_service_conflicts(df)
        after = len(df_clean)

        logger.info(
            f"Governance cleanup: removed {before - after} duplicated service records"
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cleaned_file = f"/tmp/{run_id}_history_cleaned_{timestamp}.parquet"
        df_clean.to_parquet(cleaned_file)

        return cleaned_file

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def load(db_configs, cleaned_file):
        # 1. Load the cleaned data
        df = pd.read_parquet(cleaned_file)

        # 2. CRITICAL FIX: Deduplicate by the Database Primary Key
        before_count = len(df)
        df = df.drop_duplicates(subset=['VisitServiceID'], keep='last')
        after_count = len(df)

        logger.info(f"Deduplication for SQL Load: Reduced {before_count} rows to {after_count} unique VisitServiceIDs")

        # 3. Ensure data type matching
        df['VisitServiceID'] = df['VisitServiceID'].astype(int)

        BATCH_SIZE = 1000
        total_records = len(df)
        total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info(
            f"STARTING LOAD PROCESS | {total_records} records → {total_batches} batches"
        )

        logger.info("STEP 1: Upserting data into BI Database")

        for batch_idx, start in enumerate(range(0, total_records, BATCH_SIZE), start=1):
            end = min(start + BATCH_SIZE, total_records)
            batch_df = df.iloc[start:end][["VisitServiceID", "Medical_Prediction", "Reason/Recommendation"]]

            logger.info(f"[BI Batch {batch_idx}/{total_batches}] Processing Rows {start} → {end}")

            update_table_KSA(
                db_configs["BI"],
                "AHJ_Medical_Prediction_Final",
                batch_df,
                logger,
            )

        logger.info("BI LOAD COMPLETED SUCCESSFULLY")
        logger.info("LOAD TASK FINISHED SUCCESSFULLY")

    @task
    def cleanup_files(extracted, batch_files, cleaned):
        if not batch_files:
            batch_files = []

        files_to_remove = [extracted, cleaned] + batch_files

        for file_path in files_to_remove:
            try:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")


    # --- DAG Flow ---

    # 1. Start the pipeline directly with extraction
    extracted = extract()

    # 2. Continue with the rest of the flow
    batch_files = transform(extracted)
    cleaned = clean_conflicts(batch_files)
    loaded = load(db_configs, cleaned)

    # 3. Cleanup at the very end
    loaded >> cleanup_files(extracted, batch_files, cleaned)


dag = predictions_etl_pipeline()