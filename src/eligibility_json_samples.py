import pandas as pd
import os
import json
from datetime import datetime
import time
import urllib.parse
from tqdm import tqdm

import logging
import requests
from sqlalchemy import create_engine, text

visit_id_df = pd.read_excel('/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/null_visit_ids.xlsx')
visit_id_df = visit_id_df[0:100]
visit_ids = visit_id_df["visit_id"].dropna().astype(int).tolist()

placeholders = ",".join(map(str, visit_ids))

query = f"""
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
    'ANDALUSIA HOSPITAL JEDDAH -  PROD' AS "Organization Name",
    10000000046019 AS "provider-license",
    vfi.ContractorClientPolicyNumber,
    vfi.ContractorCode AS insurer,
    vfi.InsuranceCardNo,
    vfi.ContractorClientEnName,
    vfi.ContractorClientID,
    CGWM.EnName AS purchaser_name,
    CGWM.Code AS payer_linces,
    V.CreatedDate AS original_created_date,
    V.UpdatedDate AS original_updated_date,
    GETDATE() AS extraction_timestamp
FROM VisitMgt.Visit AS v
LEFT JOIN VisitMgt.VisitFinincailInfo AS vfi ON vfi.VisitID = v.id
LEFT JOIN MPI.Patient P ON p.id = v.patientid
LEFT JOIN MPI.SLKP_Occupation OCC ON P.OccupationID = OCC.ID
LEFT JOIN MPI.SLKP_Gender G ON P.GenderID = G.ID
LEFT JOIN MPI.SLKP_MaritalStatus MS ON P.MaritalStatusID = MS.ID
LEFT JOIN Billing.Contractor BC ON BC.ID = vfi.ContractorID
LEFT JOIN MPI.LKP_IdentificationType IDY ON IDY.ID = P.IdentificationTypeID
INNER JOIN Billing.ContractorGateWayMappings CGWM
    ON CGWM.ContractorID = ISNULL(BC.ParentID, BC.ID)
    AND CGWM.GateWayID = 3
WHERE
    V.VisitStatusID != 3
    AND V.FinancialStatusID = 2
    AND V.ID IN ({placeholders})
ORDER BY V.CreatedDate ASC;
"""

logger = logging.getLogger(__name__)

with open("/home/ai/Workspace/AmrJr/eligibility-etl-airflow/passcode.json", "r") as file:
    data_dict = json.load(file)
db_names = data_dict["DB_NAMES"]


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

    try:
        logger.info("Reading data from database (Attempt 1)...")
        engine = get_conn_engine(passcode, logger)
        with engine.connect() as conn:
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        engine.dispose()
        logger.info(f"Query returned dataframe with {len(df)} rows")

    except Exception as e:
        logger.warning(f"First attempt failed: {str(e)}")
        logger.info("Retrying after 5 minutes...")
        time.sleep(300)

        try:
            engine = get_conn_engine(passcode, logger)
            with engine.connect() as conn:
                result = conn.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            engine.dispose()
            logger.info(f"Query returned dataframe with {len(df)} rows (Retry success)")

        except Exception as e:
            logger.exception(f"Second attempt failed: {str(e)}")
            raise

    if cache_path:
        try:
            df.to_excel(cache_path, index=False)
            logger.info(f"Data cached at: {cache_path}")
        except Exception as e:
            logger.warning(f"Failed to save cache: {str(e)}")

    return df


def extract_data():
    try:
        df = read_data(
            query,
            passcode=db_names["LIVE"],
            logger=logger,
            cache_path="/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/extracted_eligibility.xlsx"
        )

        if df.empty:
            print("No data found.")
            return None

        print(f"Extracted {len(df)} rows of data")
        return df

    except Exception as e:
        print(f"Error in extract_data: {str(e)}")
        raise


def transform_eligibility(extracted_data):
    """
    Transform: send each row to the API and save the raw JSON responses to
    /data/json_responses/. One file per visit_id, named <visit_id>.json.
    A manifest CSV is also written so you know which IDs were processed.
    """

    try:
        df = extracted_data.copy()

        # =========================
        # Basic date preprocessing
        # =========================
        df["start_date"] = df["start_date"].astype(str).apply(change_date)
        df["end_date"]   = df["end_date"].astype(str).apply(change_date)
        df["date_of_birth"] = df["date_of_birth"].astype(str).apply(change_date)

        print(f"Processing {len(df)} records for Eligibility transformation")

        # =========================
        # Output folder for JSON responses
        # =========================
        json_output_dir = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/json_responses"
        os.makedirs(json_output_dir, exist_ok=True)

        # =========================
        # Resume support: skip already-saved visit_ids
        # =========================
        already_saved = {
            os.path.splitext(f)[0]          # filename without .json
            for f in os.listdir(json_output_dir)
            if f.endswith(".json")
        }

        if already_saved:
            print(f"Found {len(already_saved)} already-saved responses. Skipping those visit_ids.")
            df = df[~df["visit_id"].astype(str).isin(already_saved)]
            print(f"Remaining records to process: {len(df)}")

        # =========================
        # Batch processing
        # =========================
        batch_size = 500
        manifest_rows = []

        for start in range(0, len(df), batch_size):
            end = start + batch_size
            chunk = df.iloc[start:end].copy()

            print(f"\nProcessing batch {start} → {min(end, len(df))}")

            for _, row in tqdm(chunk.iterrows(), total=len(chunk)):
                visit_id = row["visit_id"]
                payload  = create_json_payload(row, source="LIVE")
                response = send_json_to_api(payload)

                # --- Save raw JSON response ---
                response_path = os.path.join(json_output_dir, f"{visit_id}.json")
                with open(response_path, "w", encoding="utf-8") as f:
                    json.dump(response, f, ensure_ascii=False, indent=2)

                manifest_rows.append({
                    "visit_id":      visit_id,
                    "response_file": response_path,
                    "saved_at":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "status":        response.get("status", "unknown"),
                })

                time.sleep(0.1)

            print(f"Batch {start}→{min(end, len(df))} saved to {json_output_dir}")

        # =========================
        # Write manifest
        # =========================
        if manifest_rows:
            manifest_path = os.path.join(
                "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data",
                "json_responses_manifest.csv"
            )
            manifest_df = pd.DataFrame(manifest_rows)

            # Append to existing manifest if present
            if os.path.exists(manifest_path):
                existing = pd.read_csv(manifest_path)
                manifest_df = pd.concat([existing, manifest_df], ignore_index=True)

            manifest_df.to_csv(manifest_path, index=False)
            print(f"\nManifest saved at: {manifest_path}")

        print(f"\nDone. {len(manifest_rows)} JSON responses saved to: {json_output_dir}")

    except Exception as e:
        print(f"Error in transform_eligibility: {str(e)}")
        raise


def send_json_to_api(json_data):
    api_url = "http://10.111.111.6:8001/submit_eligibility"
    headers = {"Content-Type": "application/fhir+json"}
    try:
        response = requests.post(api_url, json=json_data, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"status": "error", "message": str(e)}


def create_json_payload(row, purpose="discovery", source=""):
    def safe_int_str(value, default=""):
        if pd.notna(value) and value is not None:
            try:
                return str(int(value))
            except (ValueError, TypeError):
                return default
        return default

    def safe_str(value, default=""):
        if pd.notna(value) and value is not None:
            return str(value)
        return default

    def safe_name_list(name1, name2):
        result = []
        if pd.notna(name1) and name1 is not None:
            result.append(name1)
        if pd.notna(name2) and name2 is not None:
            result.append(name2)
        return result

    identifier_system = (
        "nationalid"     if safe_str(row.get("nationality")) == "NI"
        else "passportnumber" if safe_str(row.get("nationality")) == "PPN"
        else "iqama"
    )

    json_data = {
        "purpose": purpose,
        "patient_id": safe_int_str(row.get("patient_id")),
        "payer_license": safe_int_str(row.get("payer_linces")),
        "payer_license_2": safe_int_str(row.get("payer_linces")),
        "serviced_period_start": safe_str(row.get("start_date")),
        "serviced_period_end": safe_str(row.get("end_date")),
        "created_date": safe_str(row.get("start_date")),
        "patient_name": safe_str(row.get("patient_name")),
        "patient_family_name": safe_str(row.get("family_name")),
        "patient_given_names": safe_name_list(row.get("pat_name_1"), row.get("pat_name_2")),
        "patient_gender": safe_str(row.get("gender")),
        "patient_birth_date": safe_str(row.get("date_of_birth")),
        "marital_status_code": safe_str(row.get("marital_char")),
        "patient_identifier_value": safe_int_str(row.get("iqama_no")),
        "patient_identifier_type": safe_str(row.get("nationality")),
        "patient_identifier_system": identifier_system,
        "patient_occupation_code": "others",
        "insurer_name": safe_str(row.get("purchaser_name")),
        "insurer_org_id": safe_int_str(row.get("insurer")),
    }

    return json_data


def change_date(row):
    if pd.isna(row):
        return None

    row = str(row).split(".")[0]

    try:
        date_object = datetime.strptime(row, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            date_object = datetime.strptime(row, "%Y-%m-%d")
        except ValueError:
            return None

    return date_object.strftime("%Y-%m-%d")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    df = extract_data()
    transform_eligibility(df)