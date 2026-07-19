import json
import logging
import os
import random
import time
import warnings
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

warnings.filterwarnings("ignore", category=UserWarning)

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/LCH_Visits.xlsx"
IQAMA_OUT = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/LCH_Iqama_Results.xlsx"
ELIG_OUT = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/LCH_Eligibility_Results.xlsx"
LOG_FILE = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/lch_eligibility.log"

# Checkpoint files — delete these to force a full re-run
ELIG_CHECKPOINT = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/checkpoints/checkpoint_eligibility.json"
IQAMA_CHECKPOINT = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/checkpoints/checkpoint_iqama.json"

# Temporary output files for incremental saves
ELIG_TEMP_OUT = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/LCH_Eligibility_Results_TEMP.xlsx"
IQAMA_TEMP_OUT = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/LCH_Iqama_Results_TEMP.xlsx"

IQAMA_API_URL = "http://10.111.111.6:6666/check-insurance"
ELIG_API_URL = "http://10.111.111.6:9999/submit_eligibility"

CHUNK_SIZE = 50   # save checkpoint every N rows


# -- Logging setup ---------
def setup_logger():
    logger = logging.getLogger("LCH_Eligibility")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S")
 
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
 
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
 
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
 

logger = setup_logger()


# -- Checkpoint helpers ---
def load_checkpoint(path):
    """Returns a set of already-processed keys (strings)."""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        keys = set(data.keys()) if isinstance(data, dict) else set(str(k) for k in data)
        logger.info(f"Checkpoint loaded: {path} ({len(keys)} records already done)")
        return keys
    return set()
 

def save_checkpoint(path, data):
    """Saves only the set of processed keys — no response data."""
    keys = list(data) if isinstance(data, set) else list(data.keys())
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(str(k) for k in keys), f, indent=2)
    logger.debug(f"Checkpoint saved: {path} ({len(keys)} records)")
 

def clear_checkpoint(path):
    if os.path.exists(path):
        os.remove(path)
        logger.info(f"Checkpoint cleared: {path}")


# -- Date / value helpers --
def change_date(row):
    if pd.isna(row):
        return None
    row = str(row).split(".")[0]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(row, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    logger.warning(f"Could not parse date: {row!r}")
    return None
 

def map_row(row):
    row["gender"] = {"Female": "female", "Male": "male"}.get(row["gender"], row["gender"])
    row["marital_char"] = {"Single": "U", "Married": "M"}.get(row["marital_char"], row["marital_char"])
    return row
 

def safe_str(v, default=""):
    return str(v) if pd.notna(v) and v is not None else default
 

def safe_int_str(v, default=""):
    if pd.notna(v) and v is not None:
        try:
            return str(int(float(v)))
        except (ValueError, TypeError):
            return default
    return default


# -- Payload builder ---
def create_json_payload(row, purpose="discovery"):
    identifier_system = "nationalid" if safe_str(row.get("nationality")) == "NI" else "iqama"
    return {
        "purpose": purpose,
        "patient_id":               safe_int_str(row.get("patient_id")),
        "payer_license":            safe_int_str(row.get("payer_linces")),
        "payer_license_2":          safe_int_str(row.get("payer_linces")),
        "serviced_period_start":    safe_str(row.get("start_date")),
        "created_date":             safe_str(row.get("start_date")),
        "patient_name":             safe_str(row.get("patient_name")),
        "patient_given_names":      [x for x in [row.get("pat_name_1"), row.get("pat_name_2")] if pd.notna(x) and x],
        "patient_gender":           safe_str(row.get("gender")),
        "patient_birth_date":       safe_str(row.get("date_of_birth")),
        "marital_status_code":      safe_str(row.get("marital_char")),
        "patient_identifier_value": safe_int_str(row.get("iqama_no")),
        "patient_identifier_type":  safe_str(row.get("nationality")),
        "patient_identifier_system": identifier_system,
        "patient_occupation_code":  "others",
        "insurer_name":             safe_str(row.get("purchaser_name")),
        "insurer_org_id":           safe_int_str(row.get("insurer")),
    }


# -- API callers ---
def send_eligibility(json_data, visit_id):
    try:
        r = requests.post(ELIG_API_URL, json=json_data,
                          headers={"Content-Type": "application/fhir+json"}, timeout=10)
        r.raise_for_status()
        resp = r.json()
        logger.debug(f"Eligibility visit={visit_id} → outcome={extract_outcome(resp)} | HTTP {r.status_code}")
        return resp
    except requests.exceptions.RequestException as e:
        logger.warning(f"Eligibility FAILED visit={visit_id}: {e}")
        return {"status": "error", "message": str(e)}
 

def beneficiary_api(iqama_id):
    try:
        r = requests.get(IQAMA_API_URL, params={"patient_iqama_id": int(iqama_id)}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            status = data.get("response", {}).get("ApiStatus", "Unknown") if isinstance(data, dict) else "Unknown"
            logger.debug(f"Iqama {iqama_id} → ApiStatus={status}")
            return data
        logger.warning(f"Iqama {iqama_id} → HTTP {r.status_code}")
    except Exception as e:
        logger.warning(f"Iqama API error for {iqama_id}: {e}")
    return None


# -- Response parsers ---
def extract_outcome(resp):
    if not resp or resp.get("status") != "success":
        return "Null"
    for entry in resp.get("response", {}).get("entry", []):
        outcome = entry.get("resource", {}).get("outcome")
        if outcome:
            return "Complete" if outcome == "complete" else "ERROR"
    return "Null"
 

def extract_code(resp):
    if not resp or resp.get("status") != "success":
        return None
    for entry in resp.get("response", {}).get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") == "CoverageEligibilityResponse" and res.get("outcome") == "complete":
            for ext in res.get("extension", []):
                if "extension-siteEligibility" in ext.get("url", ""):
                    for c in ext.get("valueCodeableConcept", {}).get("coding", []):
                        return c.get("code")
    return None
 

def extract_note(resp):
    if not resp or resp.get("status") != "success":
        return None
    for entry in resp.get("response", {}).get("entry", []):
        res = entry.get("resource", {})
        outcome = res.get("outcome")
        if outcome == "error":
            for err in res.get("error", []):
                for c in err.get("code", {}).get("coding", []):
                    return f"{c.get('code','')} {c.get('display','')}".strip()
        if outcome == "complete":
            return res.get("disposition")
    return None
 

def extract_benefit_value(items, benefit_code):
    """Find the allowedMoney value for a specific benefit-type code across all items."""
    for item in items:
        for benefit in item.get("benefit", []):
            codes = [c.get("code") for c in benefit.get("type", {}).get("coding", [])]
            if benefit_code in codes and "allowedMoney" in benefit:
                return benefit["allowedMoney"].get("value")
    return None
 

def parse_row(x):
    """Extract approval_limit and copay_maximum from eligibility response."""
    if not isinstance(x, dict) or x.get("status") != "success":
        return None, None
 
    entries = x.get("response", {}).get("entry", [])
    for entry in entries:
        res = entry.get("resource", {})
        if res.get("resourceType") != "CoverageEligibilityResponse":
            continue
        if res.get("outcome") != "complete":
            continue
 
        # Try insurance[0].item first (Tawuniya, Bupa, etc.)
        insurance = res.get("insurance", [])
        if insurance:
            items = insurance[0].get("item", [])
            approval_limit = extract_benefit_value(items, "approval-limit")
            copay_maximum = extract_benefit_value(items, "copay-maximum")
            return approval_limit, copay_maximum
 
        # Fallback: costToBeneficiary (Malath style)
        for ctb in res.get("costToBeneficiary", []):
            codes = [c.get("code") for c in ctb.get("type", {}).get("coding", [])]
            if "maxcopay" in codes or "copay-maximum" in codes:
                return None, ctb.get("valueMoney", {}).get("value")
 
    return None, None
 

def extract_api_status(value):
    try:
        if isinstance(value, dict):
            return value.get("response", {}).get("ApiStatus")
    except Exception:
        pass
    return None
 

def extract_insurance_data(row):
    try:
        if isinstance(row, dict) and row.get("response", {}).get("ApiStatus") == "Success":
            insurance_list = row.get("response", {}).get("Insurance", [])
            if insurance_list:
                return insurance_list[0]
    except Exception:
        pass
    return None


# -- Helper to save incremental eligibility results --
def save_eligibility_incremental(df, responses, done_keys, total_rows):
    """Save incremental eligibility results to Excel file"""
    # Create a temporary DataFrame with current results
    temp_df = df[df["visit_id"].astype(str).isin(done_keys)].copy()
    temp_df["eligibility_response"] = temp_df["visit_id"].astype(str).map(responses)
    temp_df["outcome"] = temp_df["eligibility_response"].apply(extract_outcome)
    temp_df["note"] = temp_df["eligibility_response"].apply(extract_note)
    temp_df["class"] = temp_df["eligibility_response"].apply(extract_code)
    temp_df[["approval_limit", "copay_maximum"]] = temp_df["eligibility_response"].apply(
        lambda x: pd.Series(parse_row(x))
    )
    temp_df.loc[(temp_df["note"] == "1680 ") & (temp_df["class"].isna()), "class"] = "out-network"
    temp_df.loc[(temp_df["note"] == "1658 ") & (temp_df["class"].isna()), "class"] = "not-active"
    temp_df["insertion_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    final = temp_df[["visit_id", "outcome", "note", "class", "approval_limit", "copay_maximum", "insertion_date"]]
    final.to_excel(ELIG_TEMP_OUT, index=False)
    logger.info(f"[Incremental Save] Eligibility: {len(final)}/{total_rows} rows saved to {ELIG_TEMP_OUT}")
    return final


# -- Helper to save incremental iqama results --
def save_iqama_incremental(df, responses, done_keys, total_unique):
    """Save incremental iqama results to Excel file"""
    iq_df = pd.DataFrame(responses.items(), columns=["Iqama_no", "Eligibility"])
    iq_df["api_status"] = iq_df["Eligibility"].apply(extract_api_status)
    
    ins_df = pd.concat([iq_df, iq_df["Eligibility"].apply(extract_insurance_data).apply(pd.Series)], axis=1)
    drop_cols = ["UploadDate", "NationalityCode", "Eligibility", "api_status"]
    ins_df.drop(columns=[c for c in drop_cols if c in ins_df.columns], inplace=True)
    
    df["iqama_no_int"] = pd.to_numeric(df["iqama_no"], errors="coerce").astype("Int64")
    ins_df["Iqama_no"] = pd.to_numeric(ins_df["Iqama_no"], errors="coerce").astype("Int64")
    ins_df = ins_df.merge(
        df[["iqama_no_int", "visit_id"]], left_on="Iqama_no", right_on="iqama_no_int", how="left"
    ).drop(columns=["iqama_no_int"])
    
    ins_df["Insertion_Date"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    ins_df.dropna(how="all", inplace=True)
    
    ins_df.to_excel(IQAMA_TEMP_OUT, index=False)
    logger.info(f"[Incremental Save] Iqama: {len(ins_df)}/{total_unique} unique iqamas saved to {IQAMA_TEMP_OUT}")
    return ins_df
 

# ── Section 1: Eligibility ────────────────────────────────────────────────────
def run_eligibility(df):
    logger.info("=" * 60)
    logger.info("SECTION 1/2 — ELIGIBILITY API")
    logger.info("=" * 60)
 
    # If output already exists, skip eligibility entirely and go straight to Iqama
    if os.path.exists(ELIG_OUT):
        logger.info(f"Output file already found: {ELIG_OUT}")
        logger.info("Skipping eligibility API — loading existing results and proceeding to Iqama.")
        existing = pd.read_excel(ELIG_OUT)
        logger.info(f"Loaded {len(existing)} existing eligibility rows.")
        return existing
 
    df_elig = df.copy()
    total = len(df_elig)
    logger.info(f"Total rows: {total}")
 
    done_keys = load_checkpoint(ELIG_CHECKPOINT)
    remaining = df_elig[~df_elig["visit_id"].astype(str).isin(done_keys)]
    logger.info(f"Already processed: {len(done_keys)} | Remaining: {len(remaining)}")
 
    responses = {}
    rows = list(remaining.iterrows())
    
    # Load existing responses from temp file if it exists
    if os.path.exists(ELIG_TEMP_OUT) and len(done_keys) > 0:
        try:
            temp_df = pd.read_excel(ELIG_TEMP_OUT)
            for _, row in temp_df.iterrows():
                if row["visit_id"] in done_keys:
                    # We don't have the full response, but we can mark it as processed
                    responses[str(row["visit_id"])] = {"status": "success", "cached": True}
            logger.info(f"Loaded {len(temp_df)} cached results from temp file")
        except Exception as e:
            logger.warning(f"Could not load temp file: {e}")
 
    with tqdm(total=len(rows), desc="Eligibility API", unit="visit",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        for i, (_, row) in enumerate(rows):
            visit_id = str(row["visit_id"])
            resp = send_eligibility(create_json_payload(row), visit_id)
            responses[visit_id] = resp
            done_keys.add(visit_id)
 
            pbar.set_postfix(visit=visit_id, outcome=extract_outcome(resp))
            pbar.update(1)
 
            # Save checkpoint AND incremental Excel file after every CHUNK_SIZE rows
            if (i + 1) % CHUNK_SIZE == 0:
                save_checkpoint(ELIG_CHECKPOINT, done_keys)
                save_eligibility_incremental(df_elig, responses, done_keys, total)
                logger.info(f"[Checkpoint] Eligibility: {len(done_keys)}/{total} saved")
 
    # Final save
    save_checkpoint(ELIG_CHECKPOINT, done_keys)
    final = save_eligibility_incremental(df_elig, responses, done_keys, total)
    
    # Move temp file to final output
    if os.path.exists(ELIG_TEMP_OUT):
        os.rename(ELIG_TEMP_OUT, ELIG_OUT)
        logger.info(f"Final eligibility results saved to {ELIG_OUT}")
    
    logger.info(f"Outcome summary: {final['outcome'].value_counts().to_dict()}")
    logger.info(f"Null outcomes  : {final['outcome'].isna().sum()}")
 
    clear_checkpoint(ELIG_CHECKPOINT)
    return final


# -- Section 2: Iqama ---
def run_iqama(df):
    logger.info("=" * 60)
    logger.info("SECTION 2/2 — IQAMA (BENEFICIARY) API")
    logger.info("=" * 60)
 
    unique_iqama = [iq for iq in df["iqama_no"].dropna().unique()
                    if str(iq).strip() not in ("", "nan", "None")]
    logger.info(f"Unique Iqama numbers to process: {len(unique_iqama)}")
 
    done_keys = load_checkpoint(IQAMA_CHECKPOINT)
    remaining = []
    for iq in unique_iqama:
        try:
            if str(int(float(iq))) not in done_keys:
                remaining.append(iq)
        except (ValueError, TypeError):
            logger.warning(f"Skipped non-numeric iqama during filtering: {iq!r}")
    logger.info(f"Already processed: {len(done_keys)} | Remaining: {len(remaining)}")
 
    responses = {}
    success_count = 0
    fail_count = 0
 
    with tqdm(total=len(remaining), desc="Iqama API     ", unit="iqama",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        for i, iq in enumerate(remaining):
            try:
                iq_int = int(float(iq))
                resp = beneficiary_api(iq_int)
                responses[iq_int] = resp
                done_keys.add(str(iq_int))
 
                if isinstance(resp, dict) and resp.get("response", {}).get("ApiStatus") == "Success":
                    success_count += 1
                    label = "Success"
                else:
                    fail_count += 1
                    label = "Fail"
 
                pbar.set_postfix(iqama=iq_int, status=label, ok=success_count, fail=fail_count)
                pbar.update(1)
                time.sleep(random.uniform(10, 20) / 1000)
 
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipped invalid iqama: {iq!r} — {e}")
                pbar.update(1)
 
            # Save checkpoint AND incremental Excel file after every CHUNK_SIZE rows
            if (i + 1) % CHUNK_SIZE == 0:
                save_checkpoint(IQAMA_CHECKPOINT, done_keys)
                save_iqama_incremental(df, responses, done_keys, len(unique_iqama))
                logger.info(f"[Checkpoint] Iqama: {len(done_keys)}/{len(unique_iqama)} | "
                            f"Success={success_count} Fail={fail_count}")
 
    # Final save
    save_checkpoint(IQAMA_CHECKPOINT, done_keys)
    final_ins_df = save_iqama_incremental(df, responses, done_keys, len(unique_iqama))
    
    # Move temp file to final output
    if os.path.exists(IQAMA_TEMP_OUT):
        os.rename(IQAMA_TEMP_OUT, IQAMA_OUT)
        logger.info(f"Final iqama results saved to {IQAMA_OUT}")
    
    logger.info(f"Iqama complete — Success: {success_count} | Fail: {fail_count}")
 
    clear_checkpoint(IQAMA_CHECKPOINT)
    return final_ins_df


# -- Main ---
def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("  LCH ELIGIBILITY PROCESSING STARTED")
    logger.info(f"  Started : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Input   : {INPUT_FILE}")
    logger.info(f"  Log     : {LOG_FILE}")
    logger.info(f"  Outputs : {ELIG_OUT}, {IQAMA_OUT}")
    logger.info(f"  Temp outputs : {ELIG_TEMP_OUT}, {IQAMA_TEMP_OUT}")
    logger.info(f"  Checkpoint interval: every {CHUNK_SIZE} rows")
    logger.info("=" * 60)
 
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file not found: {INPUT_FILE}")
        raise FileNotFoundError(INPUT_FILE)
 
    logger.info(f"Reading {INPUT_FILE}...")
    df = pd.read_excel(INPUT_FILE, dtype=str)
    df.columns = df.columns.str.strip()
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
 
    logger.info("Normalising date columns...")
    for col in ["start_date", "end_date", "date_of_birth"]:
        before = df[col].isna().sum()
        df[col] = df[col].apply(change_date)
        new_nulls = df[col].isna().sum() - before
        if new_nulls > 0:
            logger.warning(f"  {col}: {new_nulls} values unparseable → set to None")
        else:
            logger.info(f"  {col}: OK")
 
    logger.info("Mapping gender and marital status...")
    df = df.apply(map_row, axis=1)
 
    elig_df = run_eligibility(df)
    iqama_df = run_iqama(df)
    
    # Clean up temp files
    for temp_file in [ELIG_TEMP_OUT, IQAMA_TEMP_OUT]:
        if os.path.exists(temp_file) and os.path.exists(temp_file.replace("_TEMP", "")):
            try:
                os.remove(temp_file)
                logger.debug(f"Removed temp file: {temp_file}")
            except Exception as e:
                logger.warning(f"Could not remove temp file {temp_file}: {e}")
 
    elapsed = datetime.now() - start_time
    logger.info("=" * 60)
    logger.info("  PROCESSING COMPLETE")
    logger.info(f"  Eligibility rows : {len(elig_df)}")
    logger.info(f"  Iqama rows       : {len(iqama_df)}")
    logger.info(f"  Total time       : {str(elapsed).split('.')[0]}")
    logger.info("=" * 60)
 
 
if __name__ == "__main__":
    main()