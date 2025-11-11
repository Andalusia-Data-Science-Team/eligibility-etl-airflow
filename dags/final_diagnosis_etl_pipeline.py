from airflow.utils.email import send_email
from airflow.utils import timezone
import pendulum
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from pathlib import Path
import json
import os
import sys
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.final_diagnosis import read_data, generate_ep_key, transform_loop, update_table

with open("passcode.json", "r") as file:
    db_configs = json.load(file)
db_configs = db_configs["DB_NAMES"]
read_passcode = db_configs["BI_READ"]
live_passcode = db_configs["LIVE"]
replica_passcode = db_configs["AHJ_DOT-CARE"]

with open(Path("sql") / "final_diagnosis.sql", "r") as file:
    query = file.read()
with open(Path("sql") / "clinical_sheets.sql", "r") as file:
    cn_query = file.read()

CAIRO_TZ = pendulum.timezone("Africa/Cairo")


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
            f"[Airflow FAILURE] DAG: {dag_run.dag_id} - Task: {task_instance.task_id}"
        )
        log_url = f"http://10.24.105.221:8080/log?dag_id={dag_run.dag_id}&task_id={task_instance.task_id}&execution_date={execution_date.isoformat()}"

        # Create a more detailed HTML email body
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
            "Nadine.ElSokily@Andalusiagroup.net",
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
            # Fall back to Airflow's send_email if SMTP fails
            try:
                send_email(
                    to=[
                        "Nadine.ElSokily@Andalusiagroup.net",
                    ],
                    subject=subject,
                    html_content=html_body,
                )
                print("Used fallback email method successfully")
            except Exception as fallback_error:
                print(f"Failed to send email with fallback method: {fallback_error}")

    except Exception as e:
        print(f"Failed to process failure notification: {str(e)}")


def success_callback(context):
    """Optional success callback"""
    try:
        dag_run = context.get("dag_run")
        execution_date = context.get("execution_date")

        exec_date_str = (
            execution_date.strftime("%Y-%m-%d %H:%M:%S")
            if execution_date
            else "Unknown"
        )

        subject = f"[Airflow SUCCESS] DAG: {dag_run.dag_id} completed successfully"

        html_body = f"""
        <html>
        <body>
            <h2 style="color: #388e3c;">Airflow DAG Success</h2>
            <table border="1" cellpadding="10" cellspacing="0" style="border-collapse: collapse;">
                <tr>
                    <td><strong>DAG ID:</strong></td>
                    <td>{dag_run.dag_id}</td>
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
                    <td><strong>Status:</strong></td>
                    <td style="color: #388e3c;">SUCCESS</td>
                </tr>
            </table>
            <br>
            <p><strong>All tasks completed successfully!</strong></p>
        </body>
        </html>
        """

        send_email(
            to=[
                "Nadine.ElSokily@Andalusiagroup.net",
            ],
            subject=subject,
            html_content=html_body,
        )
        print("Success notification email sent")

    except Exception as e:
        print(f"Failed to send success notification email: {str(e)}")


# Enhanced default args with anonymous SMTP configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False,  # Set to True if you want success emails for individual tasks
    "email": [
        "Mohamed.Reda@Andalusiagroup.net",
        "Omar.Wafy@Andalusiagroup.net",
        "Asmaa.Awad@Andalusiagroup.net",
        "Andrew.Alfy@Andalusiagroup.net",
        "Nadine.ElSokily@Andalusiagroup.net",
    ],  # Default email list
    "on_failure_callback": failure_callback,
    # 'on_success_callback': success_callback,  # Uncomment if you want success emails
}


@dag(
    dag_id="final_diagnosis_job",
    default_args=default_args,
    start_date=pendulum.now(CAIRO_TZ).subtract(days=1),
    schedule_interval="30 7 * * *",
    catchup=False,
    tags=["final diagnosis", "AHJ", "medical audit"],
    max_active_runs=2,  # Prevent overlapping runs
    description="ETL pipeline for AHJ Final Diagnosis for Medical Audit team",
    # DAG-level email configuration
    params={
        "email_on_dag_failure": True,
        "notification_emails": [
            "Nadine.ElSokily@Andalusiagroup.net",
        ],
    },
)
def final_diagnosis_etl_pipeline():
    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=[
            "Mohamed.Reda@Andalusiagroup.net",
            "Omar.Wafy@Andalusiagroup.net",
            "Asmaa.Awad@Andalusiagroup.net",
            "Andrew.Alfy@Andalusiagroup.net",
            "Nadine.ElSokily@Andalusiagroup.net",
        ],
    )
    def extract():
        bi = read_data(query, read_passcode)
        bi = bi.drop_duplicates(keep="last")
        if bi.empty:
            raise AirflowSkipException("Quitting final diagnosis job")
        try:
            live = read_data(cn_query, replica_passcode)
            if live.empty:
                print(
                    "An issue was detected with the replica, reading from live database.."
                )
                live = read_data(cn_query, live_passcode)
        except Exception as e:
            print(e)
            live = read_data(cn_query, live_passcode)
        live["VisitID"] = live["VisitID"].astype(str)
        live["Episode_key"] = live.apply(
            lambda row: generate_ep_key(row["VisitID"]), axis=1
        )
        df = bi.merge(live, how="left", on="Episode_key")
        df = df.loc[~df["DiagnoseName"].isna()]

        unique_visits = df["Episode_key"].unique()
        visit_chunks = np.array_split(unique_visits, 4)
        df1 = df[df["Episode_key"].isin(visit_chunks[0])]
        df2 = df[df["Episode_key"].isin(visit_chunks[1])]
        df3 = df[df["Episode_key"].isin(visit_chunks[2])]
        df4 = df[df["Episode_key"].isin(visit_chunks[3])]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp1 = f"/tmp/extracted1_final_diagnosis_{timestamp}.parquet"
        temp2 = f"/tmp/extracted2_final_diagnosis_{timestamp}.parquet"
        temp3 = f"/tmp/extracted3_final_diagnosis_{timestamp}.parquet"
        temp4 = f"/tmp/extracted4_final_diagnosis_{timestamp}.parquet"

        df1.to_parquet(temp1)
        df2.to_parquet(temp2)
        df3.to_parquet(temp3)
        df4.to_parquet(temp4)

        return [temp1, temp2, temp3, temp4]

    @task
    def get_item(lst, idx):
        return lst[idx]

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=["Nadine.ElSokily@Andalusiagroup.net"],
    )
    def transform(extracted_data):
        extracted_data = pd.read_parquet(extracted_data)
        result_df = transform_loop(extracted_data)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df_no = np.random.randint(0, 1000)
        result_file = f"/tmp/result_{df_no}_{timestamp}.parquet"
        result_df.to_parquet(result_file)

        return result_file

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=["Nadine.ElSokily@Andalusiagroup.net"],
    )
    def load(db_configs, result_file):
        result_df = pd.read_parquet(result_file)
        update_table(db_configs["BI_READ"], "Final_Diagnosis", result_df)

    @task
    def cleanup_files(extracted_data, transformed_data):
        """Clean up the extracted data file"""
        try:
            if extracted_data and os.path.exists(extracted_data):
                os.remove(extracted_data)
                print(f"Cleaned up extraction file: {extracted_data}")

            if transformed_data and os.path.exists(transformed_data):
                os.remove(transformed_data)
                print(f"Cleaned up transformed file: {transformed_data}")

        except Exception as e:
            print(f"Error cleaning up file: {str(e)}")
            # Don't raise here as cleanup failure shouldn't fail the DAG

    # DAG flow with parallel load
    dfs = extract()

    df1 = get_item(dfs, 0)
    t1 = transform(df1)
    l1 = load(db_configs, t1)

    df2 = get_item(dfs, 1)
    t2 = transform(df2)
    l2 = load(db_configs, t2)

    df3 = get_item(dfs, 2)
    t3 = transform(df3)
    l3 = load(db_configs, t3)

    df4 = get_item(dfs, 3)
    t4 = transform(df4)
    l4 = load(db_configs, t4)

    t1 >> l1 >> cleanup_files(df1, t1)
    t2 >> l2 >> cleanup_files(df2, t2)
    t3 >> l3 >> cleanup_files(df3, t3)
    t4 >> l4 >> cleanup_files(df4, t4)


dag = final_diagnosis_etl_pipeline()
