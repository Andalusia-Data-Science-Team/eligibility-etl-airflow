import json
import logging
import re
import time
import warnings
from typing import Dict
from requests import HTTPError

import pandas as pd
import requests
from dotenv import load_dotenv
import os

warnings.filterwarnings("ignore")
_ = load_dotenv()

import tiktoken

ENCODER = tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    if not isinstance(text, str):
        text = str(text)
    return len(ENCODER.encode(text))


logger = logging.getLogger("debug_logs")
logger.setLevel(logging.DEBUG)


DEFAULT_OPENROUTER_MODEL = "deepseek/deepseek-chat-v3.1"


class OpenRouterConfigurationError(RuntimeError):
    """Raised when OpenRouter rejects the request due to auth/config issues."""


def _clean_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None:
        return None
    return value.strip().strip("\"'")


def _extract_error_details(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text.strip() or "No response body returned"

    if isinstance(payload, dict):
        error = payload.get("error", payload)
        if isinstance(error, dict):
            message = error.get("message") or error.get("metadata") or error
            return json.dumps(message, ensure_ascii=False)
        return json.dumps(error, ensure_ascii=False)

    return json.dumps(payload, ensure_ascii=False)


prompt = """
You are an expert medical assistant.
You will be provided with a patient's information in a visit and the ordered medications, lab tests and imagings in this visit.

Think about the ordered services and their purposes, and whether they align with the diagnosis. Be strict, do not make speculations on the patient's
information and history beyond the what you're provided with, stick to the explicit information that you have.

Given the patient's case and diagnosis, and using your medical knowledge, decide for each requested service whether it's "Approved" (when it's medically
necessary, for treatment or necessary pain relief, or to rule out possible risk factors, etc..), or "Rejected" (when not medically justified).

Then, only for the services that you reject, you are supposed to recommend a valid alternative service that would be a better fit for this patient's case. IF the service is medically justified but does not align with the provided diagnosis,suggest a more suitable ICD-10 code that would justify the service. You can also recommend for the doctor what to include in his diagnosis
Else if there is no valid alternative, or there is another ordered service that serves the same purpose, just say that.

Return the result in a JSON format that looks like:
Rejected:
{"127658": medicine is not indicated in this case of ..., Do you suspect ...? Please note it in your diagnosis},
{"135987": imaging is not needed, You have ordered service x that serves the same purpose}

Even if two services have the same rejection reason, clarify each of them separately in a key-value pair of its own.
Do not mention or justify any "Approved" services. If all services are approved, return an empty JSON like: Rejected: {}
Clarify your reasons in a friendly advice/recommendation tone.
Return ONLY the raw JSON object. Do not wrap it in markdown code blocks. Your response must start with { and end with }. Do not include ```json or ``` markers.
Return ONLY rejected services, do NOT return any approved services or explain their necessity.

If you cannot generate a valid JSON, return exactly:
{"Rejected": {}}
Do not return null under any circumstances.
"""


def dev_response(info, services, model=DEFAULT_OPENROUTER_MODEL):
    """
    Makes predictions on all services in a visit using OpenRouter API.
    """
    api_key = _clean_env("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY not found in environment variables")

    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": "Patient Information " + str(info)},
        {"role": "user", "content": "Ordered Services: " + str(services)},
    ]

    input_text = prompt + str(info) + str(services)
    input_tokens = count_tokens(input_text)

    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.0,
        "max_tokens": 3000,
        "response_format": {"type": "json_object"},
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": _clean_env("OPENROUTER_SITE_URL", "https://localhost"),
        "X-OpenRouter-Title": _clean_env("OPENROUTER_SITE_NAME", "Medical Predictions"),
    }

    try:
        start = time.time()

        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            data=json.dumps(payload),
            timeout=120,
        )

        end = time.time()
        elapsed = end - start

        if not response.ok:
            error_details = _extract_error_details(response)
            logger.error(
                "OpenRouter request failed with status=%s model=%s details=%s",
                response.status_code,
                model,
                error_details,
            )
            response.raise_for_status()

        data = response.json()

        full_response = data["choices"][0]["message"]["content"]

        if not isinstance(full_response, str):
            logger.warning(f"[NON-STRING RESPONSE] Converting to string: {type(full_response)}")
            full_response = json.dumps(full_response)

        usage = data.get("usage", {})
        input_tokens = usage.get("prompt_tokens", input_tokens)

        if "completion_tokens" in usage:
            output_tokens = usage["completion_tokens"]
        else:
            try:
                output_tokens = count_tokens(full_response)
            except Exception as e:
                logger.warning(f"Token counting failed, fallback to 0: {e}")
                output_tokens = 0

        logger.info(f"[MODEL NAME] {model}")
        logger.info(f"[LLM OUTPUT] {full_response}")

        return elapsed, full_response, input_tokens, output_tokens

    except HTTPError as e:
        status_code = e.response.status_code if e.response is not None else None
        error_details = (
            _extract_error_details(e.response)
            if e.response is not None
            else str(e)
        )
        if status_code in {401, 403}:
            raise OpenRouterConfigurationError(
                f"OpenRouter rejected the request (status {status_code}). {error_details}"
            ) from e
        logger.exception(f"Error generating AI response: {e}")
        raise
    except Exception as e:
        logger.exception(f"Error generating AI response: {e}")
        raise


def validate_keys(multiple):
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
                    temp[i.strip()] = v

        multiple.update(temp)
        for ele in to_del:
            del multiple[ele]

        return multiple

    except Exception as e:
        logger.exception(f"Error validating keys: {e}")
        raise


def extract_json_from_response(response: str, visit_id=None) -> dict:
    """
    Multi-strategy JSON extractor for malformed LLM output.

    Tries in order:
      1. Direct parse
      2. Strip markdown fences, then parse
      3. Brace-match to find the first complete {...} block
      4. Regex fallback — extract numeric_id: reason pairs
      5. Return empty Rejected dict as safe fallback (never raises, never marks failed)
    """
    tag = f"visit={visit_id} " if visit_id else ""

    if not response or response.strip() in ("null", ""):
        logger.warning(f"[JSON {tag}] Empty/null response, returning empty Rejected")
        return {"Rejected": {}}

    # Strategy 1: direct parse
    try:
        parsed = json.loads(response)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    # Strategy 2: strip markdown fences
    try:
        cleaned = re.sub(
            r"^```(?:json)?\s*|\s*```$", "", response.strip(), flags=re.MULTILINE
        ).strip()
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            logger.debug(f"[JSON {tag}] Parsed after stripping fences")
            return parsed
    except json.JSONDecodeError:
        pass

    # Strategy 3: brace-match to find the first complete {...} block
    try:
        start_idx = response.index("{")
        depth = 0
        for i, ch in enumerate(response[start_idx:], start=start_idx):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = response[start_idx: i + 1]
                    parsed = json.loads(candidate)
                    if isinstance(parsed, dict):
                        logger.debug(f"[JSON {tag}] Parsed via brace-matching")
                        return parsed
                    break
    except (ValueError, json.JSONDecodeError):
        pass

    # Strategy 4: regex — pull out  numeric_id: "reason"  or  numeric_id: {reason}  pairs
    try:
        pairs = re.findall(
            r'"?(\d{5,})"?\s*:\s*["{]?\s*([^,}\n"\']{10,})',
            response,
        )
        if pairs:
            rejected = {k.strip(): v.strip() for k, v in pairs}
            logger.warning(
                f"[JSON {tag}] Recovered {len(rejected)} rejection(s) via regex "
                f"fallback from malformed response"
            )
            return {"Rejected": rejected}
    except Exception:
        pass

    # Strategy 5: give up gracefully — treat as all-approved, do NOT mark as failed
    logger.error(
        f"[JSON {tag}] All parse strategies exhausted. "
        f"Treating as no rejections. Raw response: {response}"
    )
    return {"Rejected": {}}


def validate_outcome(rejected):
    to_del = []
    for key, value in rejected.items():
        value_str = str(value)
        if bool(re.search(r'\bapproved\b', value_str, re.IGNORECASE)):
            to_del.append(key)
    for key in to_del:
        del rejected[key]
    return rejected


def duplicate_services(duplications):
    predictions = {}
    for s in duplications:
        predictions[s] = "Duplicated Service"
    return predictions


def request_loop(df):
    responses: Dict[int, str] = {}
    total_time: list[float] = []
    failed_visits: list[int] = []
    total_input_tokens = 0
    total_output_tokens = 0
    visits = df["VisitID"].unique()

    for v in visits:
        if df.loc[df["VisitID"] == v, "ICD10"].isnull().any():
            responses.update(
                {
                    service: "Diagnosis was not found"
                    for service in df.loc[df["VisitID"] == v, "VisitServiceID"]
                }
            )
        else:
            try:
                row = df[df["VisitID"] == v].iloc[0]
                selected_cols = [
                    col
                    for col in row.index
                    if pd.notna(row[col])
                    and col in [
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
                    v_services = v_services.drop_duplicates(keep="first")
                    dups = list(
                        set(df.loc[df["VisitID"] == v, "VisitServiceID"])
                        - set(v_services.index)
                    )
                    if dups:
                        responses.update(duplicate_services(dups))

                res_time, answer, in_tokens, out_tokens = dev_response(
                    p_info, v_services.to_dict(orient="index")
                )
                total_input_tokens += in_tokens
                total_output_tokens += out_tokens

                logger.debug(f"Response received in {res_time:.2f} seconds for visit {v}")
                logger.info(f"[TOKENS] Visit={v} | in={in_tokens} | out={out_tokens} | total={in_tokens + out_tokens}")
                total_time.append(res_time)

            except OpenRouterConfigurationError as e:
                logger.error("Stopping batch due to OpenRouter configuration error: %s", e)
                raise
            except Exception as e:
                logger.error(f"Error processing visit {v}: {e}")
                failed_visits.append(v)
                time.sleep(60)
                continue

            # ── Unified JSON parsing (replaces fragile two-try block) ──────────
            parsed = extract_json_from_response(answer, visit_id=v)
            rejections = validate_outcome(parsed.get("Rejected", {}))
            responses.update(rejections)

    return responses, total_time, failed_visits, total_input_tokens, total_output_tokens


def make_preds(df):
    logger.info(
        f"Query returned {len(df)} rows with {df['VisitID'].nunique()} unique visits"
    )
    responses, total_time, failed_visits, total_input_tokens, total_output_tokens = request_loop(df)

    try:
        if failed_visits:
            logger.debug(f"Retrying on Failed Visits: {failed_visits}")
            failed_responses, failed_total_time, failed_visits, retry_input, retry_output = request_loop(
                df[df["VisitID"].isin(failed_visits)]
            )
            if failed_responses:
                responses.update(failed_responses)
            total_time = total_time + failed_total_time
            total_input_tokens += retry_input
            total_output_tokens += retry_output
    except Exception as e:
        logger.debug(f"Error running failed visits: {e}")

    logger.debug(f"Failed Visits: {failed_visits}")
    logger.info(f"Inference time: {sum(total_time):.2f} seconds")

    total_tokens = total_input_tokens + total_output_tokens

    INPUT_PRICE_PER_TOKEN = 0.15 / 1_000_000
    OUTPUT_PRICE_PER_TOKEN = 0.75 / 1_000_000

    total_cost = (
        total_input_tokens * INPUT_PRICE_PER_TOKEN +
        total_output_tokens * OUTPUT_PRICE_PER_TOKEN
    )

    metrics = {
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "total_tokens": total_tokens,
        "total_cost": total_cost,
    }

    history_df = write_preds(validate_keys(responses), df, failed_visits)
    return history_df, metrics


def write_preds(responses, df, failed_visits):
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
        logger.debug(f"Created prediction dataframe with {len(pred_df)} rejected services")
        df = df.merge(pred_df, on="VisitServiceID", how="left")
    else:
        logger.info("No rejected services found, all will be marked as Approved")
        df["Medical_Prediction"] = "Approved"
        df[reason_col] = None

    df["Medical_Prediction"] = df["Medical_Prediction"].fillna("Approved")
    df.loc[df["VisitID"].isin(failed_visits), "Medical_Prediction"] = "Failed to reach LLM"
    df.loc[df["VisitID"].isin(failed_visits), reason_col] = "API Error or Timeout"

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

    logger.info(f"Final prediction dataframe has {len(history_df)} rows")
    return history_df
