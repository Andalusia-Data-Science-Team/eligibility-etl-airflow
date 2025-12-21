import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_fireworks import ChatFireworks
from sqlalchemy import create_engine, text
from tqdm import tqdm

warnings.filterwarnings("ignore")
_ = load_dotenv()

# Configure debug logger
logger = logging.getLogger("debug_logs")
logger.setLevel(logging.DEBUG)


prompt = """
You are an expert medical assistant.
You will be provided with a patient's information in a visit and the ordered medications, lab tests and imagings in this visit.

Think about the ordered services and their purposes, and whether they align with the diagnosis. Be strict, do not make speculations on the patient's
information and history beyond the what you're provided with, stick to the explicit information that you have.

Given the patient's case and diagnosis, and using your medical knowledge, decide for each requested service whether it's "Approved" (when it's medically
necessary, for treatment or necessary pain relief, or to rule out possible risk factors, etc..), or "Rejected" (when not medically justified).

Then, only for the services that you reject, you are supposed to recommend a valid alternative service that would be a better fit for this patient's
case and would be classified as accepted. You can also recommend for the doctor what to include in his diagnosis or notes to justify the rejected services.
Else if there is no valid alternative, or there is another ordered service that serves the same purpose, just say that.

Return the result in a JSON format that looks like:
Rejected:
{"127658": medicine is not indicated in this case of ..., Do you suspect ...? Please note it in your diagnosis},
{"135987": imaging is not needed, You have ordered service x that serves the same purpose}

Even if two services have the same rejection reason, clarify each of them separately in a key-value pair of its own.
Do not mention or justify any "Approved" services. If all services are approved, return an empty JSON like: Rejected: {}
Clarify your reasons in a friendly advice/recommendation tone.
Return ONLY the raw JSON object. Do not wrap it in markdown code blocks. Your response must start with { and end with }. Do not include ```json or ``` markers.
"""

schema = {
    "type": "object",
    "properties": {
        "Rejected": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "Service ID": {
                        "type": "integer",
                        "description": "Reason for rejecting the service",
                    }
                },
                "required": ["Service ID"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["Rejected"],
    "additionalProperties": False,
}


def dev_response(info, services, model="accounts/fireworks/models/deepseek-v3p1"):
    """
    Makes predictions on all services in a visit and returns only the rejected ones and their rejection reason.

    Args:
    - info (dict): A dictionary containing patient's information (age, gender, diagnose..).
    - services (dict): A dictionary containing all services in a visit and their ID's.
    - model (str): Name of the model used from Fireworks.

    Returns:
    - elapsed (float): Model's response time in seconds.
    - response (str): Model's text response.
    """
    try:
        json_model = ChatFireworks(
            model=model,
            temperature=0.0,
            max_tokens=11000,
            model_kwargs={"top_k": 1, "stream": True},
            request_timeout=(120, 120),
        ).bind(response_format={"type": "json_object", "schema": schema})

        chat_history = [
            SystemMessage(content=prompt),
            HumanMessage(content="Patient Information " + str(info)),
            HumanMessage(content="Ordered Services: " + str(services)),
        ]

        start = time.time()
        stream = json_model.stream(chat_history)

        full_response = ""

        for chunk in stream:
            # Access the content from each chunk
            if hasattr(chunk, "content") and chunk.content:
                full_response += chunk.content
        end = time.time()
        elapsed = end - start

        return elapsed, full_response
    except Exception as e:
        logger.exception(f"Error generating AI response: {e}")
        raise


def validate_keys(multiple):
    """
    Checks that each key in responses dictionary do not contain multiple services if keys are returned as strings.
    If any was found, it gets split into multiple items each representing one VisitServiceID.

    Args:
    - multiple (list[int]): A dictionary VisitServiceID's as keys and rejection reasons as values.

    Returns:
    - multiple (list[int]): The input dictionary after processing.
    """
    try:
        to_del = []
        temp = {}

        for k, v in multiple.items():
            if not isinstance(k, str):
                continue

            keys = k.split(",")
            if len(keys) > 1:
                logger.debug(f"Found multi-key entry: {k}")
                to_del.append(k)
                for i in keys:
                    temp[i.strip()] = v  # strip whitespace if present

        multiple.update(temp)
        for ele in to_del:
            del multiple[ele]

        return multiple

    except Exception as e:
        logger.exception(f"Error validating keys: {e}")
        raise


def clean_llm_json(response: str) -> dict:
    """Parse JSON from LLM, handling markdown code fences."""
    # Remove markdown code fences if present
    cleaned = re.sub(
        r"^```(?:json)?\s*|\s*```$", "", response.strip(), flags=re.MULTILINE
    )
    return cleaned


def duplicate_services(duplications):
    predictions = {}
    for s in duplications:
        predictions[s] = "Duplicated Service"
    return predictions


def request_loop(df):
    responses: Dict[int, str] = {}
    total_time: list[float] = []
    failed_visits: list[int] = []
    visits = df["VisitID"].unique()

    for v in tqdm(visits, desc="Processing"):
        if (
            df.loc[df["VisitID"] == v, "ICD10"].isnull().any()
        ):  # Auto reject visits without a diagnose to avoid unnecessary requests
            responses.update(
                {
                    service: "Diagnosis was not found"
                    for service in df.loc[df["VisitID"] == v, "VisitServiceID"]
                }
            )
        else:
            try:
                row = df[df["VisitID"] == v].iloc[0]
                # Select only features that are not null to avoid entering empty problem note and symptoms to the model
                selected_cols = [
                    col
                    for col in row.index
                    if pd.notna(row[col])
                    and col
                    in [
                        "AGE",
                        "PATIENT_GENDER",
                        "CHIEF_COMPLAINT",
                        "PROVIDER_DEPARTMENT",
                        "ICD10",
                        "DIAGNOS_NAME",
                        "ProblemNote",
                        "Symptoms",
                    ]
                ]
                p_info = row[selected_cols].to_dict()
                v_services = df.loc[
                    df["VisitID"] == v,
                    ["VisitServiceID", "Service_Name", "Quantity"],
                ].set_index("VisitServiceID")

                if df.loc[df["VisitID"] == v, "Visit_Type"].iloc[0] == "Outpatient":
                    # Auto reject duplicated services to avoid unnecessary input in requests in case of outpatient only
                    v_services = v_services.drop_duplicates(keep="first")
                    dups = list(
                        set(df.loc[df["VisitID"] == v, "VisitServiceID"])
                        - set(v_services.index)
                    )
                    if dups:
                        responses.update(duplicate_services(dups))

                res_time, answer = dev_response(
                    p_info, v_services.to_dict(orient="index")
                )
                logger.debug(
                    f"Response received in {res_time:.2f} seconds for visit {v}"
                )
                total_time.append(res_time)

            except (
                Exception
            ) as e:  # Errors due to API provider, server busy for example
                logger.error(f"Error processing visit {v}: {e}")
                failed_visits.append(v)
                time.sleep(60)
                continue

            try:
                responses.update(json.loads(answer).get("Rejected", {}))
            except (
                json.JSONDecodeError
            ):  # Errors due to JSON parsing, JSON schema error or unterminated string for example
                try:
                    cleaned = clean_llm_json(answer)
                    responses.update(json.loads(cleaned).get("Rejected", {}))
                except json.JSONDecodeError as e:
                    failed_visits.append(v)
                    logger.error(
                        f"Failed to parse JSON response for visit {v}: {e}, Raw response: {answer}"
                    )
    return responses, total_time, failed_visits


def make_preds(df):
    logger.info(
        f"Query returned {len(df)} rows with {df['VisitID'].nunique()} unique visits"
    )
    responses, total_time, failed_visits = request_loop(df)

    try:
        # Retry on failed visits one time
        if failed_visits:
            logger.debug(f"Retrying on Failed Visits: {failed_visits}")
            failed_responses, failed_total_time, failed_visits = request_loop(
                df[df["VisitID"].isin(failed_visits)]
            )
            if failed_responses:
                responses.update(failed_responses)
            total_time = total_time + failed_total_time
    except Exception as e:
        logger.debug(f"Error running failed visits: {e}")

    logger.debug(f"Failed Visits: {failed_visits}")
    logger.info(f"Inference time: {sum(total_time):.2f} seconds")

    # Drop failed before merging predictions and filling nulls to avoid them getting marked as Approved
    df.drop(df[df["VisitID"].isin(failed_visits)].index, inplace=True)

    return write_preds(validate_keys(responses), df)


def write_preds(responses, df):
    df["VisitServiceID"] = df["VisitServiceID"].astype(int)
    reason_col = "Reason/Recommendation"

    if responses:
        pred_df = pd.DataFrame(
            [
                {
                    "VisitServiceID": int(k),
                    reason_col: v,
                    "Medical_Prediction": "Rejected",
                }
                for k, v in responses.items()
            ]
        )
        logger.debug(
            f"Created prediction dataframe with {len(pred_df)} rejected services"
        )
        df = df.merge(pred_df, on="VisitServiceID", how="left")
    else:
        logger.info("No rejected services found, all will be marked as Approved")
        df["Medical_Prediction"] = "Approved"
        df[reason_col] = None

    df["Medical_Prediction"] = df["Medical_Prediction"].fillna("Approved")

    history_df = df[
        [
            "VisitID",
            "VisitServiceID",
            "Service_Name",
            "Medical_Prediction",
            reason_col,
            "Diagnose",
            "Chief_Complaint",
            "ProblemNote",
            "Symptoms",
        ]
    ]

    logger.info(f"Final prediction dataframe has {len(pred_df)} rows")

    return history_df
