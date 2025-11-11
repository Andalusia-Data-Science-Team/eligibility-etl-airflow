# src/resubmission_engine.py
from __future__ import annotations

import ast
import time
import warnings
from typing import Dict, Tuple, Optional

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from langchain_fireworks import ChatFireworks
from langchain_core.messages import HumanMessage, SystemMessage

warnings.filterwarnings("ignore")
_ = load_dotenv()

PROMPT = """
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
Convince the insurance company with its validity despite the possibility of interaction with the other drug, in addition to what's above.
Implicitly, make it clear that it was taken into consideration, patient's health is a priority, and there is no harm doing this service.

If you're provided with multiple reasons, focus only on the medical ones (not technical). Address the insurer directly, no conclusion.
Return JSON:
{{"Justifications": {{"127658": "text...", "135987": "text..."}}}}
"""

SCHEMA = {
    "type": "object",
    "properties": {
        "Justifications": {
            "type": "object",
            "additionalProperties": {
                "type": "string",
                "description": "Justification text for the rejected service"
            }
        }
    },
    "required": ["Justifications"],
    "additionalProperties": False,
}

def _json_model(model_name: str) -> ChatFireworks:
    return ChatFireworks(
        model=model_name,
        temperature=0.2,
        max_tokens=5000,
        model_kwargs={"top_k": 1},
        request_timeout=(120, 120),
    ).bind(response_format={"type": "json_object", "schema": SCHEMA})

def generate_justification(
    visit_data_tuple: Tuple[str, str, Optional[str]],
    len_rejected: int,
    model: str = "accounts/fireworks/models/deepseek-v3p1",
) -> str:
    json_model = _json_model(model)
    chat_history = [
        SystemMessage(content=PROMPT),
        SystemMessage(content=f"You must return {len_rejected} justifications, one per rejected service ID."),
        HumanMessage(content=str(visit_data_tuple)),
    ]
    return json_model.invoke(chat_history).content

def data_prep(df: pd.DataFrame) -> Tuple[Tuple[str, str, Optional[str]], int]:
    patient_cols = ['Gender', 'Age', 'Diagnosis', 'ICD10', 'ProblemNote', 'Chief_Complaint', 'Symptoms']
    patient_info = str(df[patient_cols].iloc[0].dropna().to_dict())

    all_services_part = None
    if (df['Reason'] == 'High alert Drug to drug interaction / Drug combination is contra-indicated').any():
        codes = ['MN-1-1', 'AD-3-5', 'AD-1-4']
        all_services_list = df['Service_Name'].tolist()
        rejected_names = df.loc[df['ResponseReasonCode'].isin(codes), 'Service_Name'].tolist()
        other_services = list(set(all_services_list) - set(rejected_names))
        rejected_records = df.loc[df['ResponseReasonCode'].isin(codes), ['VisitServiceID', 'Service_Name', 'Note', 'Reason']].to_dict(orient='records')
        all_services_part = f"Other ordered services: {other_services}"
    else:
        rejected_records = df[['VisitServiceID', 'Service_Name', 'Note', 'Reason']].dropna(axis=1).to_dict(orient='records')

    payload = (patient_info, f"Rejected services: {rejected_records}")
    if all_services_part is not None:
        payload = (patient_info, f"Rejected services: {rejected_records}", all_services_part)

    return payload, len(rejected_records)

def final_table(result_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    merged = result_df.merge(df, on='VisitServiceID', how='left')
    if 'Status' in merged.columns:
        merged = merged.loc[merged['Status'].astype(str).str.lower() != 'approved']

    preferred_cols = [
        'RequestTransactionID', 'VisitID', 'StatementId', 'Sequence',
        'Service_id', 'Justification', 'Reason', 'Service_Name',
        'VisitStartDate', 'ContractorEnName', 'VisitClassificationEnName',
        'VisitServiceID', 'ResponseReasonCode'
    ]
    cols = [c for c in preferred_cols if c in merged.columns]
    if not cols:
        cols = [c for c in ['VisitID', 'VisitServiceID', 'Service_Name', 'Reason', 'Justification'] if c in merged.columns]
    return merged[cols]

def transform_loop(df: pd.DataFrame, logger) -> pd.DataFrame:
    data_dict: Dict[int, str] = {}
    visits = df['VisitID'].unique()
    logger.info(f"Resubmission source has {len(df)} services across {len(visits)} unique visits")
    failed_visits = []

    for v in tqdm(visits, desc="Resubmission – Processing"):
        sub = df.loc[df['VisitID'] == v]
        payload, len_rej = data_prep(sub)

        for attempt in range(2):
            try:
                raw = generate_justification(payload, len_rej)
                parsed = ast.literal_eval(raw) if isinstance(raw, str) else raw
                # tolerate edge case: model returns a one-element list with the dict
                if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
                    parsed = parsed[0]
                if not isinstance(parsed, dict):
                    raise ValueError("LLM returned non-dict JSON.")
                justifs = parsed.get("Justifications", {})
                if isinstance(justifs, dict):
                    data_dict.update(justifs)
                break
            except Exception as e:
                logger.warning(f"[Visit {v}] LLM error (attempt {attempt+1}): {e}")
                time.sleep(60)
        else:
            failed_visits.append(v)

    if failed_visits:
        logger.warning(f"Failed visits (no justification): {failed_visits}")

    result_df = pd.DataFrame(list(data_dict.items()), columns=['VisitServiceID', 'Justification'])
    if not result_df.empty:
        result_df['VisitServiceID'] = result_df['VisitServiceID'].astype(int)

    return final_table(result_df, df)

__all__ = ["transform_loop", "data_prep", "final_table", "generate_justification"]