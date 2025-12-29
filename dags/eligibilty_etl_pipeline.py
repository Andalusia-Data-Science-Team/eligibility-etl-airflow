import logging
import os
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import text
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.eligibility import (Iqama_table, change_date, create_json_payload,
                             extract_code, extract_note, extract_outcome,
                             map_row, parse_row,
                             send_json_to_api, update_table)
from src.etl_utils import get_conn_engine, email_list, START_DATE, failure_callback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log"),  # Log to a file
        logging.StreamHandler(),  # Log to the console
    ],
)

logger = logging.getLogger(__name__)

with open("passcode.json", "r") as file:
    data_dict = json.load(file)
db_names = data_dict["DB_NAMES"]

# Enhanced default args with anonymous SMTP configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False,  # Set to True if you want success emails for individual tasks
    "email": email_list,  # Default email list
    "on_failure_callback": failure_callback,
}


@dag(
    dag_id="eligibility_job_",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval="0 23,4,8,12,16,20 * * *",  # 12:00 PM, 4 AM, 8 AM, 12 PM, 4 PM, 8 PM
    catchup=False,
    tags=["eligibility", "dotcare", "parallel"],
    max_active_runs=3,  # Prevent overlapping runs
    description="ETL pipeline for eligibility data from DOT-CARE with parallel processing",
    # DAG-level email configuration
    params={
        "email_on_dag_failure": True,
        "notification_emails": email_list,
    },
)
def eligibility_etl_pipeline():

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def extract_data(**context):
        """Extract data with overlap handling to prevent data gaps"""
        try:
            query_path = (
                Path(__file__).resolve().parent.parent
                / "sql"
                / "eligibility_enhanced.sql"
            )
            query = query_path.read_text()
            current_time = datetime.now()
            engine = get_conn_engine(db_names["Replica"], logger)

            try:
                with engine.connect() as conn:
                    result = conn.execute(text(query))
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
            finally:
                engine.dispose()

            if df.empty:
                print("No new data to process.")
                return None

            print(f"Extracted {len(df)} records")

            # For large datasets, save to file instead of XCom to enable parallel processing
            timestamp = current_time.strftime("%Y%m%d_%H%M%S")
            temp_file = f"/tmp/extracted_data_{timestamp}.parquet"
            df.to_parquet(temp_file)

            return {"file_path": temp_file, "record_count": len(df)}

        except Exception as e:
            print(f"Error in extract_data: {str(e)}")
            raise

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def transform_iqama(extraction_info):
        """Transform data for Iqama table"""
        try:
            if not extraction_info:
                return None

            # Read from file instead of XCom for better performance
            df = pd.read_parquet(extraction_info["file_path"])

            print(f"Processing {len(df)} records for Iqama transformation")

            # Apply transformations
            df_iqama = (
                df.apply(map_row, axis=1).drop_duplicates().reset_index(drop=True)
            )
            df_iqama = df_iqama.drop_duplicates(keep="last").reset_index(drop=True)

            result_df = Iqama_table(df_iqama, logger)
            result_df["Insertion_Date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

            # Save transformed data to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"/tmp/iqama_transformed_{timestamp}.parquet"
            result_df.to_parquet(output_file)

            return {"file_path": output_file, "record_count": len(result_df)}

        except Exception as e:
            print(f"Error in transform_iqama: {str(e)}")
            raise

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def transform_eligibility(extraction_info):
        """Transform data for Eligibility table"""
        try:
            if not extraction_info:
                return None

            # Read from file instead of XCom for better performance
            df = pd.read_parquet(extraction_info["file_path"])
            df["insertion_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

            print(f"Processing {len(df)} records for Eligibility transformation")

            # Apply date transformations
            df["start_date"] = df["start_date"].astype(str).apply(change_date)
            df["end_date"] = df["end_date"].astype(str).apply(change_date)
            df["date_of_birth"] = df["date_of_birth"].astype(str).apply(change_date)

            # Process API requests with progress bar
            tqdm.pandas(desc="API Requests")
            df["eligibility_response"] = df.progress_apply(
                lambda row: send_json_to_api(
                    create_json_payload(row, source="Replica")
                ),
                axis=1,
            )

            # Extract response data
            df["class"] = df["eligibility_response"].apply(extract_code)
            df["outcome"] = df["eligibility_response"].apply(extract_outcome)
            df["note"] = df["eligibility_response"].apply(extract_note)
            df[["approval_limit", "copay_maximum"]] = df["eligibility_response"].apply(
                lambda x: pd.Series(parse_row(x))
            )

            # Apply business rules
            df.loc[(df["note"] == "1680 ") & (df["class"].isna()), "class"] = (
                "out-network"
            )
            df.loc[(df["note"] == "1658 ") & (df["class"].isna()), "class"] = (
                "not-active"
            )

            print(f"The number of the null values equal: {df['outcome'].isna().sum()}")

            final_df = df[
                [
                    "visit_id",
                    "outcome",
                    "note",
                    "class",
                    "approval_limit",
                    "copay_maximum",
                    "insertion_date",
                ]
            ]

            # Save transformed data to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"/tmp/eligibility_transformed_{timestamp}.parquet"
            final_df.to_parquet(output_file)

            return {"file_path": output_file, "record_count": len(final_df)}

        except Exception as e:
            print(f"Error in transform_eligibility: {str(e)}")
            raise

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=email_list,
    )
    def load_data(iqama_info, eligibility_info):
        """Load transformed data to destinations"""
        try:
            if not iqama_info and not eligibility_info:
                print("No data to load")
                return

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

            # Create output directory
            os.makedirs("data/DOT-CARE", exist_ok=True)

            # Process Iqama data
            if iqama_info:
                iqama_df = pd.read_parquet(iqama_info["file_path"])
                iqama_df.to_csv(f"data/DOT-CARE/iqama_{timestamp}.csv", index=False)
                update_table(passcodes=db_names["BI"], table_name="Iqama_dotcare", df=iqama_df, logger=logger)
                print(f"Loaded {len(iqama_df)} Iqama records")

            # Process Eligibility data
            if eligibility_info:
                eligibility_df = pd.read_parquet(eligibility_info["file_path"])
                eligibility_df.to_csv(
                    f"data/DOT-CARE/eligibilty_{timestamp}.csv", index=False
                )
                update_table(passcodes=db_names["BI"], table_name="Eligibility_dotcare", df=eligibility_df, logger=logger)
                print(f"Loaded {len(eligibility_df)} Eligibility records")

        except Exception as e:
            print(f"Error in load_data: {str(e)}")
            raise

    @task
    def cleanup_extraction_file(extraction_info, eligibility_info, iqama_info):
        """Clean up the extracted data file"""
        try:
            if extraction_info and os.path.exists(extraction_info["file_path"]):
                os.remove(extraction_info["file_path"])
                print(f"Cleaned up extraction file: {extraction_info['file_path']}")

            if eligibility_info and os.path.exists(eligibility_info["file_path"]):
                os.remove(eligibility_info["file_path"])
                print(f"Cleaned up extraction file: {eligibility_info['file_path']}")

            if iqama_info and os.path.exists(iqama_info["file_path"]):
                os.remove(iqama_info["file_path"])
                print(f"Cleaned up extraction file: {iqama_info['file_path']}")
        except Exception as e:
            print(f"Error cleaning up extraction file: {str(e)}")
            # Don't raise here as cleanup failure shouldn't fail the DAG

    # DAG flow with parallel transforms
    extracted = extract_data()

    # These two transforms will run in parallel since they don't depend on each other
    iqama_transformed = transform_iqama(extracted)
    eligibility_transformed = transform_eligibility(extracted)

    # Load data after both transforms complete
    loaded = load_data(iqama_transformed, eligibility_transformed)

    # Clean up extraction file after loading is complete
    loaded >> cleanup_extraction_file(
        extracted, iqama_transformed, eligibility_transformed
    )


dag = eligibility_etl_pipeline()
