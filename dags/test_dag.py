from airflow.utils.email import send_email
from airflow.utils import timezone
import pendulum
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import json
import os
from sqlalchemy import text
import sys
import logging
from typing import Dict, Any, Optional
import hashlib

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils import (
    Iqama_table,
    get_conn_engine,
    map_row,
    change_date,
    send_json_to_api,
    create_json_payload,
    extract_code,
    extract_outcome,
    extract_note,
    update_table,
)

CAIRO_TZ = pendulum.timezone("Africa/Cairo")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def failure_callback(context):
    """Enhanced failure callback with SMTP email sending"""
    try:
        dag_run = context.get("dag_run")
        task_instance = context.get("task_instance")
        exception = context.get("exception")
        execution_date = context.get("execution_date")

        # Format execution date properly
        exec_date_str = (
            execution_date.strftime("%Y-%m-%d %H:%M:%S")
            if execution_date
            else "Unknown"
        )

        subject = (
            f"[AIRFLOW FAILURE] DAG: {dag_run.dag_id} - Task: {task_instance.task_id}"
        )

        html_body = f"""
        <html>
        <body>
            <h2 style="color: #d32f2f;">Airflow DAG Failure Alert</h2>
            <table border="1" cellpadding="10" cellspacing="0" style="border-collapse: collapse;">
                <tr>
                    <td><strong>DAG ID:</strong></td>
                    <td>{dag_run.dag_id}</td>
                </tr>
                <tr>
                    <td><strong>Task ID:</strong></td>
                    <td>{task_instance.task_id}</td>
                </tr>
                <tr>
                    <td><strong>Execution Date:</strong></td>
                    <td>{exec_date_str}</td>
                </tr>
                <tr>
                    <td><strong>Run ID:</strong></td>
                    <td>{dag_run.run_id}</td>
                </tr>
                <tr>
                    <td><strong>State:</strong></td>
                    <td style="color: #d32f2f;">{task_instance.state}</td>
                </tr>
                <tr>
                    <td><strong>Exception:</strong></td>
                    <td><pre style="background-color: #f5f5f5; padding: 10px;">{str(exception) if exception else 'No exception details available'}</pre></td>
                </tr>
            </table>
            <br>
            <p><strong>Please investigate this failure and take appropriate action.</strong></p>
        </body>
        </html>
        """

        # List of recipients
        recipients = [
            "Mohamed.Reda@Andalusiagroup.net",
            "Omar.Wafy@Andalusiagroup.net",
            "Asmaa.Awad@Andalusiagroup.net",
            "Andrew.Alfy@Andalusiagroup.net",
            "Shehata.Amr@Andalusiagroup.net",
        ]

        # Send email using SMTP
        try:
            print("Sending failure notification via SMTP...")
            server = smtplib.SMTP("aws-ex-07.andalusia.loc", 25)
            server.set_debuglevel(1)
            server.starttls()

            # Create message
            msg = MIMEMultipart()
            msg["From"] = "ai-service@andalusiagroup.net"
            msg["To"] = ", ".join(recipients)
            msg["Subject"] = subject
            msg.attach(MIMEText(html_body, "html"))

            # Send without authentication
            server.send_message(msg)
            server.quit()
            print("✅ Failure notification sent successfully!")

        except Exception as smtp_error:
            print(f"❌ Failed to send failure notification via SMTP: {smtp_error}")

    except Exception as e:
        print(f"Failed to process failure notification: {str(e)}")


# Enhanced default args with better retry strategy
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False,
    "email": [
        "Mohamed.Reda@Andalusiagroup.net",
        "Omar.Wafy@Andalusiagroup.net",
        "Asmaa.Awad@Andalusiagroup.net",
        "Andrew.Alfy@Andalusiagroup.net",
        "Shehata.Amr@Andalusiagroup.net",
    ],
    "on_failure_callback": failure_callback,
    "execution_timeout": timedelta(hours=1),  # Global timeout for all tasks
}


@dag(
    dag_id="eligibility_job_enhanced",
    default_args=default_args,
    start_date=pendulum.now(CAIRO_TZ).subtract(days=1),
    schedule_interval="0 */4 * * *",  # Every 4 hours
    catchup=True,  # Enable catchup to handle missed runs
    tags=["eligibility", "dotcare", "parallel", "enhanced"],
    max_active_runs=1,
    description="Enhanced ETL pipeline with 24-hour coverage guarantee",
    dagrun_timeout=timedelta(hours=2),
)
def eligibility_etl_enhanced():

    @task
    def extract_data_with_overlap(**context):
        """Extract data with 30-minute overlap to prevent gaps"""
        try:
            source = "LIVE"

            # Enhanced SQL query with overlap
            enhanced_query = """
            SELECT   
                V.ID AS visit_id,
                V.CreatedDate AS start_date,
                V.UpdatedDate AS end_date,
                P.ID AS patient_id,
                CONVERT(DATE,P.DateOfBirth) AS date_of_birth,
                P.OccupationID,
                OCC.EnName AS Occupation,
                CONCAT(P.EnFirstName, ' ', P.EnSecondName, ' ', P.EnThridName, ' ', P.EnLastName) AS patient_name,
                P.EnLastName AS family_name,
                P.EnFirstName AS pat_name_1,
                P.EnSecondName AS pat_name_2,
                CASE WHEN G.CODE='m' THEN 'male' else 'female' end AS gender,
                P.NationalityID,
                CASE 
                    WHEN MS.CODE = 'm' THEN 'M'
                    WHEN MS.CODE = 's' THEN 'S'
                    WHEN MS.CODE = 'd' THEN 'D'
                    WHEN MS.CODE = 'w' THEN 'W'
                    WHEN MS.CODE = 'a' THEN 'A'
                    WHEN MS.CODE = 'c Law' THEN 'C'
                    WHEN MS.CODE = 'g' THEN 'G'
                    WHEN MS.CODE = 'p' THEN 'P'
                    WHEN MS.CODE = 'r' THEN 'R'
                    WHEN MS.CODE = 'e' THEN 'E'
                    WHEN MS.CODE = 'n' THEN 'N'
                    WHEN MS.CODE = 'i' THEN 'I'
                    WHEN MS.CODE = 'b' THEN 'B'
                    WHEN MS.CODE = 'u' THEN 'U'
                    WHEN MS.CODE = 'o' THEN 'O'
                    WHEN MS.CODE = 't' THEN 'T'
                    ELSE 'U'
                END AS marital_char,
                CASE P.IdentificationTypeID 
                    WHEN 2 THEN 'NI'
                    WHEN 3 THEN 'PPN'
                    WHEN 5 THEN 'PRC'
                    WHEN 8 THEN 'BORD'
                    ELSE 'VISA' 
                END AS nationality,
                IDY.ENNAME,
                P.IdentificationValue AS iqama_no,
                1 AS Organization_Code,
                'ANDALUSIA HOSPITAL JEDDAH -  PROD' AS 'Organization Name',
                10000000046019 AS 'provider-license',
                vfi.ContractorClientPolicyNumber,
                vfi.ContractorCode AS insurer,
                vfi.InsuranceCardNo,
                vfi.ContractorClientEnName,
                vfi.ContractorClientID,
                CGWM.EnName AS purchaser_name,
                CGWM.Code AS payer_linces
            FROM VisitMgt.Visit AS v
            LEFT JOIN VisitMgt.VisitFinincailInfo AS vfi ON vfi.VisitID = v.id
            LEFT JOIN MPI.Patient P ON p.id = v.patientid
            LEFT JOIN MPI.SLKP_Occupation OCC ON P.OccupationID = OCC.ID
            LEFT JOIN MPI.SLKP_Gender G ON P.GenderID = G.ID
            LEFT JOIN MPI.SLKP_MaritalStatus MS ON P.MaritalStatusID = MS.ID
            LEFT JOIN Billing.Contractor BC ON BC.ID = vfi.ContractorID 
            LEFT JOIN MPI.LKP_IdentificationType IDY ON IDY.ID = P.IdentificationTypeID
            INNER JOIN Billing.ContractorGateWayMappings CGWM ON CGWM.ContractorID = ISNULL(BC.ParentID, BC.ID) AND CGWM.GateWayID = 3
            WHERE V.VisitStatusID != 3 
                AND V.FinancialStatusID = 2
                AND CONVERT(DATE, V.CreatedDate) >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
                AND CONVERT(DATE, V.CreatedDate) = CONVERT(DATE, GETDATE())
                AND CONVERT(DATETIME, V.CreatedDate) >= DATEADD(MINUTE, -270, GETDATE())  -- 4.5 hours with 30-minute overlap
            ORDER BY V.CreatedDate ASC
            """

            current_time = datetime.now()
            engine = get_conn_engine(source=source)

            try:
                with engine.connect() as conn:
                    result = conn.execute(text(enhanced_query))
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
            finally:
                engine.dispose()

            if df.empty:
                print("No new data to process.")
                return None

            # Remove duplicates based on visit_id (keep latest)
            original_count = len(df)
            df = df.drop_duplicates(subset=["visit_id"], keep="last").reset_index(
                drop=True
            )

            print(
                f"Extracted {original_count} records, {len(df)} unique records after deduplication"
            )

            # Save to file for parallel processing
            timestamp = current_time.strftime("%Y%m%d_%H%M%S")
            temp_file = f"/tmp/extracted_data_{timestamp}.parquet"
            df.to_parquet(temp_file)

            return {
                "file_path": temp_file,
                "record_count": len(df),
                "original_count": original_count,
                "duplicates_removed": original_count - len(df),
            }

        except Exception as e:
            print(f"Error in extract_data_with_overlap: {str(e)}")
            raise

    @task
    def transform_iqama(extraction_info):
        """Transform data for Iqama table"""
        try:
            if not extraction_info:
                return None

            df = pd.read_parquet(extraction_info["file_path"])
            print(f"Processing {len(df)} records for Iqama transformation")

            # Apply transformations
            df_iqama = (
                df.apply(map_row, axis=1).drop_duplicates().reset_index(drop=True)
            )
            df_iqama = df_iqama.drop_duplicates(keep="last").reset_index(drop=True)

            result_df = Iqama_table(df_iqama)
            result_df["Insertion_Date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

            # Save transformed data to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"/tmp/iqama_transformed_{timestamp}.parquet"
            result_df.to_parquet(output_file)

            return {"file_path": output_file, "record_count": len(result_df)}

        except Exception as e:
            print(f"Error in transform_iqama: {str(e)}")
            raise

    @task
    def transform_eligibility(extraction_info):
        """Transform data for Eligibility table"""
        try:
            if not extraction_info:
                return None

            df = pd.read_parquet(extraction_info["file_path"])
            print(f"Processing {len(df)} records for Eligibility transformation")

            # Apply date transformations
            df["start_date"] = df["start_date"].astype(str).apply(change_date)
            df["end_date"] = df["end_date"].astype(str).apply(change_date)
            df["date_of_birth"] = df["date_of_birth"].astype(str).apply(change_date)

            # Process API requests with progress bar
            tqdm.pandas(desc="API Requests")
            df["elgability_response"] = df.progress_apply(
                lambda row: send_json_to_api(
                    create_json_payload(row, source="AHJ_DOT-CARE")
                ),
                axis=1,
            )

            # Extract response data
            df["class"] = df["elgability_response"].apply(extract_code)
            df["outcome"] = df["elgability_response"].apply(extract_outcome)
            df["note"] = df["elgability_response"].apply(extract_note)
            df["insertion_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

            # Apply business rules
            df.loc[(df["note"] == "1680 ") & (df["class"].isna()), "class"] = (
                "out-network"
            )
            df.loc[(df["note"] == "1658 ") & (df["class"].isna()), "class"] = (
                "not-active"
            )

            null_outcomes = df["outcome"].isna().sum()
            print(f"The number of null outcomes: {null_outcomes}")

            final_df = df[["visit_id", "outcome", "note", "class", "insertion_date"]]

            # Save transformed data to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"/tmp/eligibility_transformed_{timestamp}.parquet"
            final_df.to_parquet(output_file)

            return {
                "file_path": output_file,
                "record_count": len(final_df),
                "null_outcomes": null_outcomes,
            }

        except Exception as e:
            print(f"Error in transform_eligibility: {str(e)}")
            raise

    @task
    def load_data(iqama_info, eligibility_info):
        """Load transformed data to destinations"""
        try:
            if not iqama_info and not eligibility_info:
                print("No data to load")
                return

            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

            # Create output directory
            os.makedirs("data/DOT-CARE", exist_ok=True)

            load_summary = {
                "timestamp": timestamp,
                "iqama_loaded": 0,
                "eligibility_loaded": 0,
            }

            # Process Iqama data
            if iqama_info:
                iqama_df = pd.read_parquet(iqama_info["file_path"])
                iqama_df.to_csv(f"data/DOT-CARE/iqama_{timestamp}.csv", index=False)
                update_table(table_name="Iqama_dotcare", df=iqama_df)
                load_summary["iqama_loaded"] = len(iqama_df)
                print(f"Loaded {len(iqama_df)} Iqama records")

            # Process Eligibility data
            if eligibility_info:
                eligibility_df = pd.read_parquet(eligibility_info["file_path"])
                eligibility_df.to_csv(
                    f"data/DOT-CARE/eligibilty_{timestamp}.csv", index=False
                )
                update_table(table_name="Eligibility_dotcare", df=eligibility_df)
                load_summary["eligibility_loaded"] = len(eligibility_df)
                print(f"Loaded {len(eligibility_df)} Eligibility records")

            print(f"Load Summary: {load_summary}")
            return load_summary

        except Exception as e:
            print(f"Error in load_data: {str(e)}")
            raise

    @task
    def cleanup_files(extraction_info, eligibility_info, iqama_info):
        """Clean up temporary files"""
        try:
            files_to_clean = []

            if extraction_info and extraction_info.get("file_path"):
                files_to_clean.append(extraction_info["file_path"])
            if eligibility_info and eligibility_info.get("file_path"):
                files_to_clean.append(eligibility_info["file_path"])
            if iqama_info and iqama_info.get("file_path"):
                files_to_clean.append(iqama_info["file_path"])

            for file_path in files_to_clean:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Cleaned up file: {file_path}")

        except Exception as e:
            print(f"Error cleaning up files: {str(e)}")

    # DAG flow
    extracted = extract_data_with_overlap()

    # Parallel transforms
    iqama_transformed = transform_iqama(extracted)
    eligibility_transformed = transform_eligibility(extracted)

    # Load data
    loaded = load_data(iqama_transformed, eligibility_transformed)

    # Cleanup
    cleanup_task = cleanup_files(extracted, iqama_transformed, eligibility_transformed)

    # Set dependencies
    loaded >> cleanup_task


dag = eligibility_etl_enhanced()
