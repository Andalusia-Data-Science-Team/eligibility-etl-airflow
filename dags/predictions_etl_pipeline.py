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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.AHJ_Claims import read_data, make_preds, update_table

with open("passcode.json", "r") as file:
    db_configs = json.load(file)
db_configs = db_configs["DB_NAMES"]
bi_write_passcode = db_configs["BI"]
ai_write_passcode = db_configs["AI"]

CAIRO_TZ = pendulum.timezone('Africa/Cairo')


def failure_callback(context):
    """Enhanced failure callback with SMTP email sending"""
    try:
        dag_run = context.get("dag_run")
        task_instance = context.get("task_instance")
        exception = context.get("exception")
        execution_date = context.get('execution_date')
        
        # Format execution date properly
        exec_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S') if execution_date else 'Unknown'
        
        subject = f"[Airflow FAILURE] DAG: {dag_run.dag_id} - Task: {task_instance.task_id}"
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
            'Nadine.ElSokily@Andalusiagroup.net',
        ]

        # Send email using SMTP
        try:
            print("Sending failure notification via SMTP...")
            server = smtplib.SMTP('aws-ex-07.andalusia.loc', 25)
            server.set_debuglevel(1)
            server.starttls()
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = 'ai-service@andalusiagroup.net'
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send without authentication
            server.send_message(msg)
            server.quit()
            print("✅ Failure notification sent successfully!")
            
        except Exception as smtp_error:
            print(f"❌ Failed to send failure notification via SMTP: {smtp_error}")
            # Fall back to Airflow's send_email if SMTP fails
            try:
                send_email(
                    to=['Nadine.ElSokily@Andalusiagroup.net',],
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
        execution_date = context.get('execution_date')
        
        exec_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S') if execution_date else 'Unknown'
        
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
            to=['Mohamed.Reda@Andalusiagroup.net',
                'Omar.Wafy@Andalusiagroup.net',
                'Asmaa.Awad@Andalusiagroup.net',
                'Andrew.Alfy@Andalusiagroup.net',
                'Nadine.ElSokily@Andalusiagroup.net',],
            subject=subject,
            html_content=html_body,
        )
        print("Success notification email sent")
        
    except Exception as e:
        print(f"Failed to send success notification email: {str(e)}")


# Enhanced default args with anonymous SMTP configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': False,  # Set to True if you want success emails for individual tasks
    'email': ['Mohamed.Reda@Andalusiagroup.net',
              'Omar.Wafy@Andalusiagroup.net',
              'Asmaa.Awad@Andalusiagroup.net',
              'Andrew.Alfy@Andalusiagroup.net',
              'Nadine.ElSokily@Andalusiagroup.net',],  # Default email list
    'on_failure_callback': failure_callback,
    # 'on_success_callback': success_callback,  # Uncomment if you want success emails
}


@dag(
    dag_id="predictions_job",
    default_args=default_args,
    start_date=pendulum.now(CAIRO_TZ).subtract(days=1),
    schedule_interval='0 0,4,8,12,16,20 * * *',  # Every 4 hours
    catchup=False,
    tags=["predictions", "dotcare", "parallel"],
    max_active_runs=10,  # Prevent overlapping runs
    description="ETL pipeline for AHJ Claims Medical Predictions from DOT-CARE with parallel processing",
    # DAG-level email configuration
    params={
        'email_on_dag_failure': True,
        'notification_emails': ['Nadine.ElSokily@Andalusiagroup.net',]
    }
)
def predictions_etl_pipeline():
    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=['Mohamed.Reda@Andalusiagroup.net',
               'Omar.Wafy@Andalusiagroup.net',
               'Asmaa.Awad@Andalusiagroup.net',
               'Andrew.Alfy@Andalusiagroup.net',
               'Nadine.ElSokily@Andalusiagroup.net',]
    )
    def extract():
        df = read_data()
        if df.empty:
            raise AirflowSkipException("Query returned 0 rows, quitting predictions job")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_file = f"/tmp/extracted_claims_{timestamp}.parquet"
        df.to_parquet(temp_file)
        return temp_file
    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=['Nadine.ElSokily@Andalusiagroup.net']
    )
    def transform(extracted_data):
        extracted_data = pd.read_parquet(extracted_data)
        history_df = make_preds(extracted_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        history_file = f"/tmp/history_{timestamp}.parquet"
        history_df.to_parquet(history_file)
        return history_file
    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=['Nadine.ElSokily@Andalusiagroup.net']
    )
    def load(db_configs, history):
        history = pd.read_parquet(history)
        pred_df = history[["VisitServiceID", "Medical_Prediction", "Reason/Recommendation"]]
        update_table(db_configs["BI"], "AHJ_Predictions_DotCare", pred_df)
        update_table(db_configs["AI"], "Claims_Predictions_History", history)
    @task
    def cleanup_extraction_file(extracted_data, transformed_data):
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

    # DAG flow with parallel load
    extracted = extract()
    
    transformed = transform(extracted)

    load = load(db_configs, transformed)

    load >> cleanup_extraction_file(extracted, transformed)

dag = predictions_etl_pipeline()