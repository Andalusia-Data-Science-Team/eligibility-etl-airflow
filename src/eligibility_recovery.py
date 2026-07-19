import pandas as pd
import os
import glob
import json
from datetime import datetime
import time
import urllib.parse
import pyodbc
from tqdm import tqdm

import logging
import requests
from sqlalchemy import create_engine, text

visit_id_df = pd.read_excel('/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/null_visit_ids.xlsx')
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


def read_data(query, passcode, logger, cache_path=None, force_refresh=False):
    """
    Executes a SQL query using get_conn_engine with:
    - Retry mechanism
    - Optional Excel caching

    Args:
        query (str): SQL query
        passcode: DB connection identifier
        logger: logger object
        cache_path (str, optional): path to Excel cache file
        force_refresh (bool): if True, ignore cache and re-fetch from DB

    Returns:
        pandas DataFrame
    """

    # =========================
    # 1. LOAD FROM CACHE
    # =========================
    if cache_path and os.path.exists(cache_path) and not force_refresh:
        try:
            logger.info(f"Loading data from cache: {cache_path}")
            df = pd.read_excel(cache_path)
            logger.info(f"Loaded {len(df)} rows from cache")
            return df
        except Exception as e:
            logger.warning(f"Failed to read cache. Will fetch from DB. Error: {str(e)}")

    # =========================
    # 2. FETCH FROM DB (TRY 1)
    # =========================
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

        # =========================
        # 3. FETCH FROM DB (TRY 2)
        # =========================
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

    # =========================
    # 4. SAVE TO CACHE
    # =========================
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
    """Transform data for Eligibility table with batching + checkpoints + resume support"""

    try:
        df = extracted_data.copy()

        # =========================
        # Basic preprocessing
        # =========================
        df["insertion_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        print(f"Processing {len(df)} records for Eligibility transformation")

        df["start_date"] = df["start_date"].astype(str).apply(change_date)
        df["end_date"] = df["end_date"].astype(str).apply(change_date)
        df["date_of_birth"] = df["date_of_birth"].astype(str).apply(change_date)

        # =========================
        # Check existing checkpoints (RESUME LOGIC)
        # =========================
        checkpoint_dir = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data"
        checkpoint_files = glob.glob(f"{checkpoint_dir}/checkpoint_*.csv")

        processed_ids = set()

        if checkpoint_files:
            print(f"Found {len(checkpoint_files)} checkpoint files. Loading processed IDs...")

            for f in checkpoint_files:
                try:
                    temp_df = pd.read_csv(f)
                    if "visit_id" in temp_df.columns:
                        processed_ids.update(temp_df["visit_id"].tolist())
                except Exception as e:
                    print(f"Skipping checkpoint {f} due to error: {e}")

            print(f"Already processed records: {len(processed_ids)}")

            # Remove already processed rows
            df = df[~df["visit_id"].isin(processed_ids)]
            print(f"Remaining records after resume filter: {len(df)}")

        # =========================
        # Batch processing
        # =========================
        batch_size = 500
        all_results = []

        for start in range(0, len(df), batch_size):
            end = start + batch_size
            chunk = df.iloc[start:end].copy()

            print(f"\nProcessing batch {start} → {end}")

            responses = []

            for _, row in tqdm(chunk.iterrows(), total=len(chunk)):
                payload = create_json_payload(row, source="LIVE")

                response = send_json_to_api(payload)
                responses.append(response)

                time.sleep(0.1)  # safe now

            chunk["eligibility_response"] = responses

            # =========================
            # Extract fields
            # =========================
            chunk["class"] = chunk["eligibility_response"].apply(extract_code)
            chunk["outcome"] = chunk["eligibility_response"].apply(extract_outcome)
            chunk["note"] = chunk["eligibility_response"].apply(extract_note)

            chunk[["approval_limit", "copay_maximum"]] = chunk[
                "eligibility_response"
            ].apply(lambda x: pd.Series(parse_row(x)))

            # =========================
            # Business rules
            # =========================
            chunk.loc[
                (chunk["note"] == "1680 ") & (chunk["class"].isna()),
                "class"
            ] = "out-network"

            chunk.loc[
                (chunk["note"] == "1658 ") & (chunk["class"].isna()),
                "class"
            ] = "not-active"

            # =========================
            # Final columns
            # =========================
            final_chunk = chunk[
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

            all_results.append(final_chunk)

            # =========================
            # CHECKPOINT SAVE
            # =========================
            checkpoint_path = f"{checkpoint_dir}/checkpoint_{start}_{end}.csv"
            final_chunk.to_csv(checkpoint_path, index=False)

            print(f"Saved checkpoint: {checkpoint_path}")

        # =========================
        # FINAL OUTPUT
        # =========================
        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
        else:
            final_df = pd.DataFrame()

        final_output_path = f"{checkpoint_dir}/final_transformed_eligibility.csv"
        final_df.to_csv(final_output_path, index=False)

        print(f"\nTotal transformed rows: {len(final_df)}")
        print(f"Final dataset saved at: {final_output_path}")

        return final_df

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
    # Helper function to safely convert values to int and then string
    def safe_int_str(value, default=""):
        if pd.notna(value) and value is not None:
            try:
                return str(int(value))
            except (ValueError, TypeError):
                return default
        return default

    # Helper function to safely convert values to string
    def safe_str(value, default=""):
        if pd.notna(value) and value is not None:
            return str(value)
        return default

    # Helper function to safely handle patient given names
    def safe_name_list(name1, name2):
        result = []
        if pd.notna(name1) and name1 is not None:
            result.append(name1)
        if pd.notna(name2) and name2 is not None:
            result.append(name2)
        return result

    # Determine identifier system safely
    identifier_system = (
        "nationalid" if safe_str(row.get("nationality")) == "NI"
        else "passportnumber" if safe_str(row.get("nationality")) == "PPN"
        else "iqama"
    )
    if source == "Replica":
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
            "patient_given_names": safe_name_list(
                row.get("pat_name_1"), row.get("pat_name_2")
            ),
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
    else:
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
            "patient_given_names": safe_name_list(
                row.get("pat_name_1"), row.get("pat_name_2")
            ),
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

    row = str(row).split(".")[0]  # Remove anything after a dot (like ".357000")

    try:
        # Try parsing with time first
        date_object = datetime.strptime(row, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            # If that fails, try parsing date only
            date_object = datetime.strptime(row, "%Y-%m-%d")
        except ValueError:
            # If it still fails, return None (or raise an error if you prefer)
            return None

    return date_object.strftime("%Y-%m-%d")


def extract_outcome(response):
    # Handle None responses
    if response is None:
        # print("Response is None. Cannot extract outcome.")
        return "Null"

    # Check if the status is "success"
    if response.get("status") != "success":
        # print("Status is not 'success'. Cannot proceed.")
        return "Null"

    # Safely get entries
    entries = response.get("response", {}).get("entry", [])
    if not entries:
        print("No entries found in the response.")
        return "Null"

    # Extract the value of the "outcome" key
    outcome_value = None

    for entry in entries:
        resource = entry.get("resource", {})
        outcome_value = resource.get("outcome")
        if outcome_value is not None:
            break

    if outcome_value == "complete":
        return "Complete"
    elif outcome_value == "error":
        return "ERROR"
    else:
        return "Null"


def extract_code(response):
    # Handle None responses
    if response is None:
        print("Response is None. Cannot proceed.")
        return None

    # Check if the status is "success"
    if response.get("status") != "success":
        print("Status is not 'success'. Cannot proceed.")
        return None

    # Check if the response has the expected structure
    if not isinstance(response.get("response"), dict):
        print(
            "Response format is not as expected. Missing or invalid 'response' field."
        )
        return None

    # Check if 'entry' exists in the response
    entries = response.get("response", {}).get("entry", [])
    if not entries:
        print("No entries found in the response.")
        return None

    # Loop through the entries to find CoverageEligibilityResponse
    for entry in entries:
        resource = entry.get("resource", {})
        if (
            resource.get("resourceType") == "CoverageEligibilityResponse"
            and resource.get("outcome") == "complete"
        ):
            for ext in resource.get("extension", []):
                if (
                    ext.get("url")
                    == "http://nphies.sa/fhir/ksa/nphies-fs/StructureDefinition/extension-siteEligibility"
                ):
                    coding = ext.get("valueCodeableConcept", {}).get("coding", [])
                    for code_item in coding:
                        code_value = code_item.get("code")
                        return code_value

    return None


def extract_note(response):
    disposition_value = ""

    # Handle None responses
    if response is None:
        print("Response is None. Cannot extract note.")
        return None

    # Check if the status is "success"
    if response.get("status") != "success":
        print("Status is not 'success'. Cannot proceed.")
        return None

    # Safely get entries
    entries = response.get("response", {}).get("entry", [])
    if not entries:
        print("No entries found in the response.")
        return None

    # Extract the value of the "outcome" key
    outcome_value = None
    error_value = None

    for entry in entries:
        resource = entry.get("resource", {})
        outcome_value = resource.get("outcome")

        if outcome_value == "error":
            error_value = resource.get("error")

        if outcome_value == "complete":
            disposition_value = resource.get("disposition", "")

        if outcome_value is not None:
            break

    # Print the outcome value
    if outcome_value is None:
        print("Outcome key not found in the response.")

    # If the outcome is "error", return error details
    if outcome_value == "error":
        if error_value is not None:
            for error in error_value:
                code_obj = error.get("code", {})
                coding_list = code_obj.get("coding", [])
                for coding in coding_list:
                    return f"{coding.get('code', '')} {coding.get('display', '')}"
        else:
            print("Error key not found in the resource.")
            return "Unknown error"

    # Return disposition value for complete outcomes
    elif disposition_value:
        return disposition_value
    else:
        print("Disposition key not found in the resource.")
        return None


def find_keys(data, target="allowedMoney"):
    results = []

    if isinstance(data, dict):
        for k, v in data.items():
            if k == target:
                results.append(v)
            results.extend(find_keys(v, target))
    elif isinstance(data, list):
        for item in data:
            results.extend(find_keys(item, target))

    return results


def extract_allowedMoney(results):
    # Works with Tawuniya jsons
    if not results or len(results) < 2:
        return None, None
    return results[0]["value"], results[1]["value"]


def extract_maxcopay(results):
    # Works with malath jsons
    try:
        return None, results[0][1]["valueMoney"]["value"]
    except Exception:
        return None, None


def bupa_approval_limit(json_obj):
    # Works with bupa jsons
    try:
        items = json_obj["response"]["entry"][1]["resource"]["insurance"][0]["item"]
    except Exception:
        return None, None

    for it in items:
        if it.get("name") == "Approval limit":
            # the benefit list always contains allowedMoney as first element
            ben = it.get("benefit", [])
            for b in ben:
                if "allowedMoney" in b:
                    return b["allowedMoney"]["value"], None

    return None, None


def parse_row(x):
    s = str(x)

    if "tawuniya.com.sa" in s:
        keys = find_keys(x)
        return extract_allowedMoney(keys)

    elif "www.malath.com.sa" in s:
        keys = find_keys(x, "costToBeneficiary")
        return extract_maxcopay(keys)

    elif "bupa.com.sa" in s:
        return bupa_approval_limit(x)

    else:
        return None, None


def load_data(eligibility_df=None, excel_path=None):
    """Load transformed data to destinations (supports DataFrame or Excel input)"""
    try:
        # =========================
        # 1. Load from Excel if provided
        # =========================
        if excel_path:
            print(f"Loading data from Excel: {excel_path}")
            eligibility_df = pd.read_excel(excel_path)

        # =========================
        # 2. Validate data
        # =========================
        if eligibility_df is None or eligibility_df.empty:
            print("No data to load")
            return

        # =========================
        # 3. Save backup before loading
        # =========================
        output_path = "/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/recovered_eligibility.csv"
        eligibility_df.to_csv(output_path, index=False)
        print(f"Backup saved at: {output_path}")

        # =========================
        # 4. Insert directly using pyodbc (bypasses update_table entirely)
        # =========================
        passcodes = db_names["BI"]
        server = passcodes["Server"]
        if "," not in server and "\\" not in server:
            server = f"{server},1433"

        connection_string = (
            f"DRIVER={passcodes['driver']};"
            f"SERVER={server};"
            f"DATABASE={passcodes['Database']};"
            f"UID={passcodes['UID']};"
            f"PWD={passcodes['PWD']};"
            f"Connection Timeout=60;"
            f"Encrypt=no;"
            f"TrustServerCertificate=yes;"
        )

        df_clean = eligibility_df.copy()
        df_clean = df_clean.where(pd.notnull(df_clean), None)

        if "insertion_date" in df_clean.columns:
            df_clean["insertion_date"] = pd.to_datetime(
                df_clean["insertion_date"]
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        columns = list(df_clean.columns)
        column_names = ", ".join([f"[{col}]" for col in columns])
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f"INSERT INTO dbo.[Eligibility_dotcare] ({column_names}) VALUES ({placeholders})"

        chunk_size = 1000
        total_rows = len(df_clean)
        inserted_count = 0

        for i in range(0, total_rows, chunk_size):
            chunk = df_clean.iloc[i: i + chunk_size]
            data_tuples = []
            for _, row in chunk.iterrows():
                row_tuple = []
                for value in row:
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        row_tuple.append(None)
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        row_tuple.append(value.strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        row_tuple.append(value)
                data_tuples.append(tuple(row_tuple))

            cursor.executemany(insert_sql, data_tuples)
            conn.commit()
            inserted_count += len(data_tuples)
            print(f"Inserted {inserted_count}/{total_rows} records")

        cursor.close()
        conn.close()
        print(f"Successfully loaded {inserted_count} Eligibility records")

    except Exception as e:
        print(f"Error in load_data: {str(e)}")
        raise


# =========================
# RUN
# =========================
if __name__ == "__main__":
    # df = extract_data()
    # transformed = transform_eligibility(df)
    load_data(excel_path='/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/final_transformed_eligibility.xlsx')

# # Extract response data:
# with open("/home/ai/Workspace/AmrJr/eligibility-etl-airflow/data/eligibility_response_sample2.json", "r", encoding="utf-8") as f:
#     data = json.load(f)

# df = pd.DataFrame([{
#     "class": extract_code(data),
#     "outcome": extract_outcome(data),
#     "note": extract_note(data),
#     **dict(zip(["approval_limit", "copay_maximum"], parse_row(data)))
# }])

# # Apply business rules
# df.loc[(df["note"] == "1680 ") & (df["class"].isna()), "class"] = "out-network"
# df.loc[(df["note"] == "1658 ") & (df["class"].isna()), "class"] = "not-active"

# print(f"The number of the null values equal: {df['outcome'].isna().sum()}")

# final_df = df[
#     [
#         "outcome",
#         "note",
#         "class",
#         "approval_limit",
#         "copay_maximum"
#     ]
# ]

# print(final_df.head())