import ast
import time
import warnings

import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_fireworks import ChatFireworks
from tqdm import tqdm

warnings.filterwarnings("ignore")
_ = load_dotenv()

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
Justifications:
{"127658": 'justification for service 127658..'},
{"135987": 'justification for service 135987..'}
Where keys are the service id (NOT THE SERVICE NAME/DESCRIPTION) and values are the justification for it
"""

schema = {
    "type": "object",
    "properties": {
        "Justifications": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "VisitServiceID": {
                        "type": "integer",
                        "description": "Justification for the rejected service",
                    }
                },
                "required": ["VisitService ID"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["Justifications"],
    "additionalProperties": False,
}


def generate_justification(
    visit_data, len_rejected, model="accounts/fireworks/models/deepseek-v3p1"
):
    json_model = ChatFireworks(
        model=model,
        temperature=0.2,
        max_tokens=10000,
        model_kwargs={"top_k": 1, "stream": True},
        request_timeout=(120, 120),
    ).bind(response_format={"type": "json_object", "schema": schema})

    chat_history = [
        SystemMessage(content=prompt),
        SystemMessage(
            content=f"You are supposed to return {len_rejected} justifications, for {len_rejected} rejected service/s only"
        ),
        HumanMessage(content=str(visit_data)),
    ]

    stream = json_model.stream(chat_history)
    full_response = ""

    for chunk in stream:
        # Access the content from each chunk
        if hasattr(chunk, "content") and chunk.content:
            full_response += chunk.content

    return full_response


def data_prep(df):
    patient_info = str(
        df[
            [
                "Gender",
                "Age",
                "Diagnosis",
                "ICD10",
                "ProblemNote",
                "Chief_Complaint",
                "Symptoms",
            ]
        ]
        .iloc[0]
        .dropna()
        .to_dict()
    )
    all_services = None
    if (
        df["Reason"]
        == "High alert Drug to drug interaction / Drug combination is contra-indicated"
    ).any():
        codes = ["MN-1-1", "AD-3-5", "AD-1-4"]
        all_services = df["Service_Name"].to_list()
        rejected = (
            df["Service_Name"].loc[df["ResponseReasonCode"].isin(codes)].to_list()
        )
        all_services = set(all_services) - set(rejected)
        rejected = (
            df[["VisitServiceID", "Service_Name", "Note", "Reason"]]
            .loc[df["ResponseReasonCode"].isin(codes)]
            .to_dict(orient="records")
        )
    else:
        rejected = df[["VisitServiceID", "Service_Name", "Note", "Reason"]]
        rejected = rejected = rejected.dropna(axis=1).to_dict(orient="records")

    result = (patient_info, f"Rejected services: {rejected}")
    if all_services is not None:
        result += (f"Other ordered services: {all_services}",)
    return result, len(rejected)


def transform_loop(df, logger):
    data_dict = {}
    visits = df["VisitID"].unique()
    logger.info(f"Dataframe has {len(df)} services with {len(visits)} unique visits")
    failed_visits = []
    for v in tqdm(visits, desc="Processing"):
        sub = df.loc[df["VisitID"] == v]
        visit_data, len_rej = data_prep(sub)
        try:
            response = generate_justification(visit_data, len_rej)
            data_dict.update(ast.literal_eval(response).get("Justifications", {}))
        except Exception as e:
            logger.debug(f"Error processing visit {v}: {e}")
            logger.debug("Retrying...")
            time.sleep(60)
            try:
                response = generate_justification(visit_data, len_rej)
                data_dict.update(ast.literal_eval(response).get("Justifications", {}))
            except Exception as e:
                logger.debug(f"Error processing visit {v}: {e}")
                failed_visits.append(v)
    logger.debug(f"Failed visits: {failed_visits}")
    result_df = pd.DataFrame(
        list(data_dict.items()), columns=["VisitServiceID", "Justification"]
    )
    result_df["VisitServiceID"] = result_df["VisitServiceID"].astype(int)
    return final_table(result_df, df)


def final_table(result_df, df):
    merged = result_df.merge(df, on="VisitServiceID", how="left")
    merged = merged.loc[merged["Status"] != "approved"]
    merged = merged[
        [
            "RequestTransactionID",
            "VisitID",
            "StatementId",
            "Sequence",
            "Service_id",
            "Justification",
            "Reason",
            "Service_Name",
            "VisitStartDate",
            "ContractorEnName",
            "VisitClassificationEnName",
            "VisitServiceID",
            "ResponseReasonCode",
        ]
    ]
    return merged
