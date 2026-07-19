import pandas as pd
import json
from datetime import datetime, time
from pathlib import Path
import urllib.parse

import logging
import requests
from sqlalchemy import text, create_engine

query = """
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
    AND V.ID = 911092
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


def read_data(query, passcode, logger):
    """
    Executes a SQL query using get_conn_engine. If the first attempt fails, waits for 5 minutes before trying again.

    Returns:
        pandas DataFrame with query results
    """
    try:
        df = pd.read_sql_query(query, get_conn_engine(passcode, logger))
        logger.info(f"Query returned dataframe with {len(df)} rows")
        return df
    except Exception as e:
        logger.debug(f"First attempt failed with error: {str(e)}")
        logger.debug("Waiting 5 minutes before retrying...")
        # time.sleep(300)  # Wait for 5 minutes (300 seconds)

        # Second attempt
        try:
            return pd.read_sql_query(query, get_conn_engine(passcode, logger))
        except Exception as e:
            logger.debug(f"Data extraction attempt failed with error: {str(e)}")
            logger.exception("Second attempt to execute read_sql_query failed")
            # Raise so callers can handle retry/failure properly instead of receiving None
            raise


def extract_data():
    try:
        df = read_data(query, db_names["LIVE"], logger)

        if df.empty:
            print("No data found.")
            return None

        print(df.head())
        return df

    except Exception as e:
        print(f"Error in extract_data: {str(e)}")
        raise


extract_data()


def transform_eligibility():
    """Transform data for Eligibility table"""
    try:

        df = extract_data()
        df["insertion_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        print(f"Processing {len(df)} records for Eligibility transformation")

        # Apply date transformations
        df["start_date"] = df["start_date"].astype(str).apply(change_date)
        df["end_date"] = df["end_date"].astype(str).apply(change_date)
        df["date_of_birth"] = df["date_of_birth"].astype(str).apply(change_date)

        # Process API requests with progress bar
        df["eligibility_response"] = df.apply(
            lambda row: send_json_to_api(
                create_json_payload(row, source="LIVE")
            ),
            axis=1,
        )

        df["eligibility_payload"] = df.apply(
            lambda row: create_json_payload(row, source="LIVE"),
            axis=1
        )

        # Save payloads to JSON (what you send to API)
        df["eligibility_payload"].to_json(
            "eligibility_payload.json",
            orient="records",
            indent=2,
            force_ascii=False
        )

        df['eligibility_response'].to_json(
            "eligibility_response.json",
            orient="records",
            indent=2,
            force_ascii=False)
        
        # Extract response data
        df["class"] = df["eligibility_response"].apply(extract_code)
        df["outcome"] = df["eligibility_response"].apply(extract_outcome)
        df["note"] = df["eligibility_response"].apply(extract_note)
        df[["approval_limit", "copay_maximum"]] = df["eligibility_response"].apply(
            lambda x: pd.Series(parse_row(x))
        )

        # Apply business rules
        df.loc[(df["note"] == "1680 ") & (df["class"].isna()), "class"] = "out-network"
        df.loc[(df["note"] == "1658 ") & (df["class"].isna()), "class"] = "not-active"

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

        print(final_df.head())

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
        "nationalid" if safe_str(row.get("nationality")) == "NI" else "iqama"
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


transform_eligibility()

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