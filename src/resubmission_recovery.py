import ast
import time
import warnings
import json

import urllib.parse
from sqlalchemy import create_engine, text

import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from tqdm import tqdm

import logging
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

warnings.filterwarnings("ignore")
load_dotenv(override=True)

with open("/home/ai/Workspace/AmrJr/eligibility-etl-airflow/passcode.json", "r") as file:
    db_configs = json.load(file)
db_configs = db_configs["DB_NAMES"]

with open("/home/ai/Workspace/AmrJr/eligibility-etl-airflow/sql/resubmission.sql", "r") as file:
    query = file.read()


# ─── Logging Setup ────────────────────────────────────────────────────────────
# IMPORTANT: configure logging ONCE, BEFORE any getLogger() calls

LOG_DIR = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/logs"
OUTPUT_DIR = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path = os.path.join(LOG_DIR, f"resubmission_{run_timestamp}.log")

# Set up root logger with both file and console handlers
file_handler = logging.FileHandler(log_path, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger = logging.getLogger("resubmission")
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.propagate = False  # prevent double-logging via root logger


# ─── Prompt & Schema ──────────────────────────────────────────────────────────

prompt = """
You are a medical claims expert.
You will be provided with ordered services for a patient in a visit (medication, lab test, imaging, etc..), and the patient's information.
These services were rejected by the insurance company as they claim that they're not necessary, not indicated for this diagnosis,
the patient was ordered another service that does the same purpose, the documented diagnosis is not covered by their policy,
not valid or inconsistent with the patient's age.
Think about the ordered services and their purpose. Given the patient's diagnosis and the rejection reason, and using your medical knowledge,
justify and highlight the necessity for the requested services (for treatment or necessary pain relief, or to rule out possible risk factors, etc..),
and their validity despite the rejection reason, implicitly make it clear that patient's health is a priority,
and convince the insurance company with the absolute need for these services, emphasize their importance in solid medical terms.
If this service was rejected by the insurance company because they claim that drug combination is contra-indicated
(it has severe interactions with one of the other ordered drugs), think about which drug might be the cause of the interaction in question.
convince the insurance company with its validity despite the possibility of interaction with the other drug, in addition to what's above.
Implicitly, make it clear that it was taken into consideration, patient's health is a priority, and there is no harm doing this service.
If you're provided with multiple reasons, focus only on the ones listed above (the medical ones, not technical)
Talk as if you're addressing the insurance company. Do not talk about it in third person. Do not comment on the rejection reason provided.
Do not add a conclusion at the end. Keep it in a medium length like the provided example.
You are supposed to write your justification FOR EACH service to look like the following format:
The patient presented with joint disorders (M25) and generalized fatigue and
malaise (R53). These symptoms may indicate possible underlying renal impairment, which can be associated with systemic inflammatory conditions,
autoimmune disorders, or side effects from medications used to manage joint symptoms (e.g., NSAIDs or DMARDs).
Evaluating kidney function through Serum Creatinine is essential before initiating or continuing treatment, especially when medications known to affect
renal function are considered. Additionally, unexplained fatigue (R53) may be linked to reduced renal clearance or metabolic imbalance,
further supporting the medical necessity of the test.
Therefore, this service is both clinically and diagnostically indicated to guide appropriate management and treatment.
Return your output in a valid JSON format that looks like:
{
  "Justifications": {
    "127658": "justification for service 127658...",
    "135987": "justification for service 135987..."
  }
}
Where keys are the service id (NOT THE SERVICE NAME/DESCRIPTION) and values are the justification for it
"""

schema = {
    "type": "object",
    "properties": {
        "Justifications": {
            "type": "object",
            "additionalProperties": {"type": "string"},
        }
    },
    "required": ["Justifications"],
    "additionalProperties": False,
}


# ─── Helper Functions ─────────────────────────────────────────────────────────

def get_conn_engine(passcodes, logger):
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


def read_data(query, passcode, logger, cache_path=None, force_refresh=False):
    if cache_path and os.path.exists(cache_path) and not force_refresh:
        try:
            logger.info(f"Loading data from cache: {cache_path}")
            df = pd.read_excel(cache_path)
            logger.info(f"Loaded {len(df)} rows from cache")
            return df
        except Exception as e:
            logger.warning(f"Failed to read cache. Will fetch from DB. Error: {str(e)}")

    for attempt in range(1, 3):
        try:
            logger.info(f"Reading data from database (Attempt {attempt})...")
            engine = get_conn_engine(passcode, logger)
            with engine.connect() as conn:
                result = conn.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            engine.dispose()
            logger.info(f"Query returned {len(df)} rows")
            break
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed: {str(e)}")
            if attempt == 1:
                logger.info("Retrying after 5 minutes...")
                time.sleep(300)
            else:
                logger.exception("Second attempt also failed.")
                raise

    if cache_path:
        try:
            df.to_excel(cache_path, index=False)
            logger.info(f"Data cached at: {cache_path}")
        except Exception as e:
            logger.warning(f"Failed to save cache: {str(e)}")

    return df

def clean_json_response(response: str):
    response = response.strip()

    # Remove markdown code fences
    if response.startswith("```json"):
        response = response.replace("```json", "", 1)

    if response.startswith("```"):
        response = response.replace("```", "", 1)

    if response.endswith("```"):
        response = response[:-3]

    response = response.strip()

    return json.loads(response)

# ─── ETL ──────────────────────────────────────────────────────────────────────

def extract():
    df = read_data(query, db_configs["LIVE"], logger)
    if df.empty:
        logger.info("No data was found in live, quitting resubmission job")
        return None
    df = df.drop_duplicates(keep="last")
    logger.info(f"Extracted {len(df)} rows")
    return df


def transform(extracted_df):
    if extracted_df is None or extracted_df.empty:
        logger.info("No extracted data to transform")
        return None
    result_df = transform_loop(extracted_df, logger)
    logger.info(f"Transformed {len(result_df)} rows")
    return result_df


# ─── LLM ──────────────────────────────────────────────────────────────────────

def generate_justification(visit_data, len_rejected, model="deepseek/deepseek-chat-v3.1"):
    json_model = ChatOpenAI(
        model=model,
        base_url="https://openrouter.ai/api/v1",
        api_key=os.getenv("OPENROUTER_API_KEY"),
        temperature=0.2,
        max_tokens=10000,
        streaming=False,
        timeout=120,
        extra_body={"response_format": {"type": "json_object"}},
    )
    chat_history = [
        SystemMessage(content=prompt),
        SystemMessage(
            content=f"You are supposed to return {len_rejected} justifications for {len_rejected} rejected services only."
        ),
        HumanMessage(content=str(visit_data)),
    ]
    response = json_model.invoke(chat_history)
    usage = (
        response.response_metadata.get("token_usage")
        or response.response_metadata.get("usage")
    )
    return response.content, usage


# ─── Data Prep ────────────────────────────────────────────────────────────────

def data_prep(df):
    patient_info = str(
        df[["Gender", "Age", "Diagnosis", "ICD10", "ProblemNote", "Chief_Complaint", "Symptoms"]]
        .iloc[0]
        .dropna()
        .to_dict()
    )
    all_services = None
    if (df["Reason"] == "High alert Drug to drug interaction / Drug combination is contra-indicated").any():
        codes = ["MN-1-1", "AD-3-5", "AD-1-4"]
        all_services = df["Service_Name"].to_list()
        rejected = df["Service_Name"].loc[df["ResponseReasonCode"].isin(codes)].to_list()
        all_services = set(all_services) - set(rejected)
        rejected = (
            df[["VisitServiceID", "Service_Name", "Note", "Reason"]]
            .loc[df["ResponseReasonCode"].isin(codes)]
            .to_dict(orient="records")
        )
    else:
        rejected = df[["VisitServiceID", "Service_Name", "Note", "Reason"]]
        rejected = rejected.dropna(axis=1).to_dict(orient="records")

    result = (patient_info, f"Rejected services: {rejected}")
    if all_services is not None:
        result += (f"Other ordered services: {all_services}",)
    return result, len(rejected)


# ─── Excel Helpers ────────────────────────────────────────────────────────────

CHECKPOINT_EVERY = 3
CHECKPOINT_DIR = os.path.join(OUTPUT_DIR, f"checkpoints_{run_timestamp}")
FINAL_OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"resubmission_final_{run_timestamp}.xlsx")
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def write_excel(df, path):
    """Write a DataFrame to an Excel file with formatting."""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Results")
        ws = writer.sheets["Results"]
        for col in ws.columns:
            max_len = max((len(str(cell.value)) if cell.value else 0) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 80)
        ws.freeze_panes = "A2"


def save_checkpoint(batch_df, batch_num):
    """Save a checkpoint batch to its own Excel file."""
    path = os.path.join(CHECKPOINT_DIR, f"checkpoint_batch_{batch_num:04d}.xlsx")
    write_excel(batch_df, path)
    logger.info(f"Checkpoint saved: {path} ({len(batch_df)} rows)")


def load_processed_visits():
    """Scan checkpoint files to find already-processed VisitIDs (supports resume)."""
    if not os.path.isdir(CHECKPOINT_DIR):
        return set()
    processed = set()
    for fname in os.listdir(CHECKPOINT_DIR):
        if fname.endswith(".xlsx"):
            try:
                tmp = pd.read_excel(
                    os.path.join(CHECKPOINT_DIR, fname),
                    engine="openpyxl",
                    usecols=["VisitID"],
                )
                processed.update(tmp["VisitID"].dropna().astype(int).tolist())
            except Exception as e:
                logger.warning(f"Could not read checkpoint {fname}: {e}")
    if processed:
        logger.info(f"Resuming — {len(processed)} visits already in checkpoints")
    return processed


def merge_checkpoints_to_final():
    """Merge all checkpoint Excel files into one final file."""
    files = sorted(
        f for f in os.listdir(CHECKPOINT_DIR) if f.endswith(".xlsx")
    )
    if not files:
        logger.warning("No checkpoint files found to merge.")
        return None

    frames = []
    for fname in files:
        try:
            frames.append(pd.read_excel(os.path.join(CHECKPOINT_DIR, fname), engine="openpyxl"))
        except Exception as e:
            logger.warning(f"Skipping unreadable checkpoint {fname}: {e}")

    final_df = pd.concat(frames, ignore_index=True)
    write_excel(final_df, FINAL_OUTPUT_PATH)
    logger.info(f"Final merged file saved: {FINAL_OUTPUT_PATH} ({len(final_df)} total rows)")
    return final_df


# ─── Transform Loop ───────────────────────────────────────────────────────────

def transform_loop(df, logger):
    visits = df["VisitID"].unique()
    logger.info(f"Dataframe has {len(df)} services with {len(visits)} unique visits")

    processed_visits = load_processed_visits()
    remaining_visits = [v for v in visits if v not in processed_visits]
    logger.info(f"{len(processed_visits)} visits already processed, {len(remaining_visits)} remaining")

    # Determine next batch number from existing checkpoints
    existing = [
        f for f in os.listdir(CHECKPOINT_DIR) if f.startswith("checkpoint_batch_") and f.endswith(".xlsx")
    ]
    batch_num = len(existing) + 1

    data_dict = {}
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    failed_visits = []

    for i, v in enumerate(tqdm(remaining_visits, desc="Processing visits"), start=1):
        sub = df.loc[df["VisitID"] == v]
        visit_data, len_rej = data_prep(sub)

        def run_inference():
            return generate_justification(visit_data, len_rej)

        try:
            response, usage = run_inference()
        except Exception as e:
            logger.debug(f"Error processing visit {v}: {e} — retrying in 60s...")
            time.sleep(60)
            try:
                response, usage = run_inference()
            except Exception as e2:
                logger.debug(f"Retry failed for visit {v}: {e2}")
                failed_visits.append(v)
                processed_visits.add(v)
                continue

        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        visit_total_tokens = usage.get("total_tokens", 0)

        visit_input_cost = (prompt_tokens / 1_000_000) * 0.372
        Visit_output_cost = (completion_tokens / 1_000_000) * 1.39
        visit_total_cost = visit_input_cost + Visit_output_cost

        logger.info(
            f"Visit {v} | Prompt: {prompt_tokens} | Completion: {completion_tokens} | Total: {visit_total_tokens} | Cost: Cost: ${visit_total_cost:.6f}"
        )

        parsed_response = clean_json_response(response)

        data_dict.update(
            parsed_response.get("Justifications", {})
        )
        processed_visits.add(v)

        total_prompt_tokens += prompt_tokens
        total_completion_tokens += completion_tokens
        total_tokens += visit_total_tokens

        # ── Save checkpoint batch every N visits ──────────────────────────
        if i % CHECKPOINT_EVERY == 0:
            batch_df = pd.DataFrame(list(data_dict.items()), columns=["VisitServiceID", "Justification"])
            if not batch_df.empty:
                batch_df["VisitServiceID"] = batch_df["VisitServiceID"].astype(int)
                save_checkpoint(final_table(batch_df, df), batch_num)
                batch_num += 1
                data_dict.clear()

    # ── Flush any remaining visits ────────────────────────────────────────
    if data_dict:
        batch_df = pd.DataFrame(list(data_dict.items()), columns=["VisitServiceID", "Justification"])
        batch_df["VisitServiceID"] = batch_df["VisitServiceID"].astype(int)
        save_checkpoint(final_table(batch_df, df), batch_num)

    logger.debug(f"Failed visits ({len(failed_visits)}): {failed_visits}")

    input_cost = (total_prompt_tokens / 1_000_000) * 0.372
    output_cost = (total_completion_tokens / 1_000_000) * 1.39
    total_cost = input_cost + output_cost

    logger.info(
        f"""
        =====================================
        TOTAL USAGE SUMMARY
        =====================================
        Prompt Tokens:       {total_prompt_tokens}
        Completion Tokens:   {total_completion_tokens}
        Total Tokens:        {total_tokens}
        Input Cost:          ${input_cost:.6f}
        Output Cost:         ${output_cost:.6f}
        Total Cost:          ${total_cost:.6f}
        =====================================
        """
    )

    return merge_checkpoints_to_final()


# ─── Final Table ──────────────────────────────────────────────────────────────

def final_table(result_df, df):
    merged = result_df.merge(df, on="VisitServiceID", how="left")
    merged = merged.loc[merged["Status"] != "approved"]
    merged = merged[
        [
            "RequestTransactionID", "VisitID", "StatementId", "Sequence",
            "Service_id", "Justification", "Reason", "Service_Name",
            "VisitStartDate", "ContractorEnName", "VisitClassificationEnName",
            "VisitServiceID", "ResponseReasonCode",
        ]
    ]
    return merged


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    extracted_df = extract()
    transformed_df = transform(extracted_df)

    if transformed_df is None or transformed_df.empty:
        logger.info("No transformed data to save.")