import pandas as pd
import re
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from sqlalchemy import create_engine, text
from tqdm import tqdm
from dotenv import load_dotenv
from langchain_fireworks import ChatFireworks
from langchain_core.messages import HumanMessage, SystemMessage
import warnings
import logging
from rapidfuzz import fuzz
from bs4 import BeautifulSoup
from typing import Dict

warnings.filterwarnings("ignore")
_ = load_dotenv()

# Configure debug logger
logger = logging.getLogger("final_diagnosis_logs")
logger.setLevel(logging.DEBUG)

schema = {
    "type": "object",
    "properties": {
        "Primary Diagnosis": {
            "type": "object",
            "properties": {
                "Diagnosis": {
                    "type": "string",
                    "description": "Primary diagnosis description"
                },
                "ICD10": {
                    "type": "string",
                    "description": "ICD10 code for the primary diagnosis"
                }
            },
            "required": ["Diagnosis", "ICD10"],
            "additionalProperties": False
        },
        "Secondary Diagnoses": {
            "type": "object",
            "properties": {
                "Diagnosis": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "description": "Secondary diagnosis description"
                    }
                },
                "ICD10": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "description": "ICD10 code for secondary diagnosis"
                    }
                }
            },
            "required": ["Diagnosis", "ICD10"],
            "additionalProperties": False
        }
    },
    "required": ["Primary Diagnosis"],
    "additionalProperties": False
}

prompt = """
Based on this information and your medical opinion, you are supposed to audit the given diagnoses.
Provide the final primary diagnosis and ICD10 for this patient, it doesn't have to be the same diagnosis given, you are allowed to suggest
another one if it is more accurate or aligns better with the patient's data. You can also recommend an initial diagnosis if you're not provided with any.
Provide secondary diagnoses too if exists/if necessary, and sort them by order of most relevant or important.
You are allowed to leave this field empty if there isn't any.
Return the output in a valid JSON schema like the following format:
{"Primary Diagnosis": {"Diagnosis": "Intracerebral hemorrhage, nontraumatic", "ICD10": "I61.9"},
"Secondary Diagnoses": {"Diagnosis": ["Septic shock due to Klebsiella pneumoniae","Acute kidney injury"], "ICD10": ["A41.52","N17.9"]}}
You must be 100% sure of the provided ICD10 codes. They must align with the disease descriptions.
"""


def get_conn_engine(passcodes):
    """
    Creates and returns a SQLAlchemy engine for connecting to the SQL database.

    Args:
    - passcodes (dict): A dictionary containing database credentials.

    Returns:
    - engine (sqlalchemy.engine.Engine): A SQLAlchemy Engine instance for connecting to the database.
    """
    try:
        server, db, uid, pwd, driver = (
            passcodes["Server"],
            passcodes["Database"],
            passcodes["UID"],
            passcodes["PWD"],
            passcodes["driver"],
        )
        params = urllib.parse.quote_plus(
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={db};"
        f"UID={uid};"
        f"PWD={pwd};"
        f"Connection Timeout=300;"
        )
        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
        logger.debug(f"Database connection engine created for {server}/{db}")
        return engine
    except KeyError as e:
        logger.error(f"Missing key in passcodes dictionary: {e}")
        raise
    except Exception as e:
        logger.exception(f"Error creating database connection engine: {e}")
        raise


def read_data(query, passcode):
    """
    Executes a SQL query using get_conn_engine. If the first attempt fails, waits for 5 minutes before trying again.
    
    Returns:
        pandas DataFrame with query results
    """
    try:
        df = pd.read_sql_query(query, get_conn_engine(passcode))
        logger.info(f"Query returned dataframe with {len(df)} rows")
        return df
    except Exception as e:
        logger.debug(f"First attempt failed with error: {str(e)}")
        logger.debug("Waiting 5 minutes before retrying...")
        time.sleep(300)  # Wait for 5 minutes (300 seconds)
    
        # Second attempt
        try:
            return pd.read_sql_query(query, get_conn_engine(passcode))
        except Exception as e:
            logger.debug(f"Second attempt failed with error: {str(e)}")


def update_table(passcode: Dict[str, str], table_name: str, df: pd.DataFrame, retries=28, delay=500):
    """
    Updates a database table with the given DataFrame. Retries on failure.

    Parameters:
    - table_name: Name of the table to update.
    - df: DataFrame to update the table.
    - retries: Number of retry attempts.
    - delay: Delay in seconds between retries.
    """
    try:
        engine = get_conn_engine(passcode)
        # Create a copy of the DataFrame to avoid modifying the original
        df_clean = df.copy()

        attempt = 0
        while attempt < retries:
            try:
                logger.debug(f"Update attempt {attempt+1}/{retries}")
                logger.debug("Connection established, beginning data transfer")
                df_clean.to_sql(
                    name=f"{table_name}",
                    con=engine,
                    index=False,
                    if_exists="append",
                    chunksize=1000,
                    schema="dbo",
                )
                logger.info(f"Successfully updated '{table_name}' table")
                return  # Exit the function if successful
            except Exception as e:
                attempt += 1
                logger.error(f"Attempt {attempt} failed: {str(e)}")
                if attempt < retries:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    failure_msg = "All retries failed. Please check the error and try again later."
                    logger.error(failure_msg)
                    raise  # Re-raise the exception after all retries fail
    except Exception as e:
        logger.exception(f"Critical error in updating table {table_name}: {e}")
        raise


def summarize_notes(df):
    """
    Summarize progress notes for a patient/visit using fuzzy. Gets rid of highly repetitive notes with no significant difference
    and extracts a unique set representing the notes that are informative. (Drops duplicated notes semantically)

    Args:
    - df (pd.DataFrame): A subset of the dataframe with 'Note' column representing a patient's visit.

    Returns:
    - unique_notes (list[str]): A list of strings representing summarized notes.
    """
    unique_notes: list[str] = []

    for idx, note in enumerate(df["Note"]):
        is_similar = False
        for existing in unique_notes:
            score = fuzz.token_sort_ratio(note, existing)
            if score > 50:
                is_similar = True
                break
        if not is_similar:
            unique_notes.append(note)

    return unique_notes


def data_prep(sub):
    """
    Prepares an episode's data to be entered to the LLM.

    Args:
    - vid (str): The episode key.

    Returns:
    - specialty (str): Patient's provider department.
    - data (list[str]): vital signs, progress notes, diagnosis, etc..
    """
    # sub = df.loc[df['Episode_key'] == vid]
    specialty = sub['specialty'].iloc[0]
    data = []  # a list of strings
    # Vitals
    vitals = sub[['Vital_Sign1', 'Vital_SignType']].dropna()
    vitals = vitals.drop_duplicates(subset=['Vital_Sign1', 'Vital_SignType'], keep='last')
    if not vitals.empty:
        vitals = 'Vital Signs: ' + str(vitals.set_index('Vital_SignType')['Vital_Sign1'].to_dict())
        data.append(vitals)
    # Notes
    if sub['Note'].notna().any():  # at least one note exists
        notes = summarize_notes(sub)
        data = data + notes
    # Clinical Sheet
    sheets = sub[['HTMLBody', 'Form_Type']].dropna()
    for row in sheets.itertuples(index=True):
        sheet = extract_cn(row.HTMLBody, row.Form_Type)
        if sheet is not None:
            data.append(sheet)
    # Body
    if sub['Body'].notna().any():
        data = data + list(sub['Body'].unique())
    # Diagnosis, chief complaint, symptoms, ... age and gender
    info = sub[['DiagnoseName', 'ICDDiagnoseCode', 'Age', 'Gender', 'ChiefComplaintNotes', 'SymptomNotes']].dropna(axis=1).iloc[0].to_dict()
    data.append(str(info))
    return specialty, data


def call_llm(sub, model="accounts/fireworks/models/qwen3-235b-a22b"):
    json_model = ChatFireworks(model=model, temperature=0.0, max_tokens=5000, model_kwargs={"top_k": 1}).bind(
        response_format={"type": "json_object", "schema": schema}
    )
    specialty, data = data_prep(sub)
    chat_history = [
        SystemMessage(content="You are a helpful medical expert. You will be provided with a patient's data during their visit to the hospital, the initial diagnosis and ICD10 code"),
        HumanMessage(content=f'A patient presented to the {specialty} department'),
        HumanMessage(content=str(data)),
        SystemMessage(content=prompt),
    ]
    start = time.time()
    response = json_model.invoke(chat_history).content
    end = time.time()
    elapsed = end - start
    return response, elapsed


def extract_cn(html, form_type):
    soup = BeautifulSoup(html, 'lxml')
    if form_type in ('ApprovalReport', 'MedicalReport', 'ApproverReport'):
        summary = []
        # Get all tags of interest from the entire soup, not just body
        for tag in soup.find_all(['div']):
            text = tag.get_text(" ", strip=True)
            if text and text not in summary:  # avoid duplicates
                summary.append(text)

        # To remove PII
        if form_type == 'MedicalReport':
            return summary[-3]
        else:
            return summary[-2]
    elif form_type == 'Approval':
        return soup.find('pre').get_text(" ", strip=True) if soup.find('pre') else None


def get_description(raw_response):
    primary = json.loads(raw_response).get("Primary Diagnosis", {})
    secondary = json.loads(raw_response).get("Secondary Diagnoses", {})
    return primary, secondary


def processing_thoughts(text):
    pattern = r"<think>(.*?)</think>"
    text_in_between = re.findall(pattern, str(text), re.DOTALL)
    clean_text = re.sub(pattern, "", text, flags=re.DOTALL)
    return clean_text.strip(), text_in_between[0].strip()


def transform_loop(df):
    logger.info(
        f"Resulting dataframe has {len(df)} rows with {df['Episode_key'].nunique()} unique visits"
    )
    cols = ['Primary_Diagnosis', 'Primary_ICD10', 'Secondary_Diagnoses', 'Secondary_ICD10']
    for col in cols:
        df[col] = None
    visits = df["Episode_key"].unique()
    failed_visits = []
    for v in tqdm(visits, desc="Processing"):
        sub = df.loc[df['Episode_key'] == v]
        try:
            answer, elapsed = call_llm(sub)
            logger.debug(f"Response received in {elapsed:.2f} seconds for visit {v}")
            
            response, thought = processing_thoughts(answer)
            response = response.replace("\n", "")
            
            primary, secondary = get_description(response)
            idx = df[df["Episode_key"] == v].index

            df.loc[idx, 'Primary_Diagnosis'] = primary.get("Diagnosis", "")
            df.loc[idx, 'Primary_ICD10'] = primary.get("ICD10", "")
            df.loc[idx, 'Secondary_Diagnoses'] = ',  '.join(secondary.get("Diagnosis", []))
            df.loc[idx, 'Secondary_ICD10'] = ',  '.join(secondary.get("ICD10", []))

        except Exception as e:
            logger.error(f"Error processing Episode {v}: {e}")
            failed_visits.append(v)

    return df[['Episode_key'] + cols].drop_duplicates(keep='last')


def generate_ep_key(VisitID):
    return '11_' +  VisitID
