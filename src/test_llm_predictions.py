# 1. IMPORT TRICK: Force openai to load first to prevent circular import errors
import openai 
import os
import time
import json
import re
import logging
import warnings
import uvicorn
from typing import Dict, Any

# FastAPI
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Your Libraries
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_fireworks import ChatFireworks

# --- SETUP ---
warnings.filterwarnings("ignore")
_ = load_dotenv()

# Configure Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("debug_logs")

# Initialize FastAPI
app = FastAPI(title="Predictions Logic Test", version="1.0")

# --- ORIGINAL LOGIC & PROMPTS ---

prompt = """
You are an expert medical reviewer assisting with medical approval and claims validation.

You will be provided with:
- A patient’s visit information
- The documented diagnoses for this visit
- A list of ordered services (medications, lab tests, imaging, procedures)

Your task is to identify ONLY services that must be REJECTED.
All services not explicitly rejected are considered APPROVED by default.

You must base decisions strictly on the provided information.
Do NOT speculate, infer missing details, or optimize treatment.

--------------------------------------------------
FUNDAMENTAL DECISION MODEL
--------------------------------------------------

DEFAULT APPROVAL RULE:
If an ordered service is reasonably compatible with at least ONE of the documented diagnoses
and there is no explicit contradiction, duplication, or contraindication,
it MUST be treated as APPROVED and omitted from the output.

Uncertainty or ambiguity is NOT a valid reason for rejection.

--------------------------------------------------
DIAGNOSIS ANCHORING (MANDATORY)
--------------------------------------------------

1. ONE-DIAGNOSIS-IS-ENOUGH RULE:
If a service is commonly used, approved, or reasonable for at least ONE diagnosis in the visit,
it MUST be APPROVED,
even if other diagnoses are broader, unspecified, or less clearly related.

2. DIAGNOSIS HIERARCHY RULE:
When both a specific diagnosis and an unspecified or general diagnosis are present,
the specific diagnosis takes precedence.
Do NOT reject a service due to the presence of a broader or unspecified diagnosis
when a compatible specific diagnosis exists.

3. NO INDICATION RE-LITIGATION:
Do NOT debate or reinterpret clinical indication nuances
when a compatible diagnosis is already present.

--------------------------------------------------
MEDICATION-SPECIFIC RULES (CRITICAL)
--------------------------------------------------

1. NO DRUG CLASS INFERENCE:
If a medication name does NOT explicitly state its pharmacological class
and no class metadata is provided,
you MUST NOT guess or infer:
- drug class
- mechanism of action
- therapeutic intent

Do NOT use speculative language such as:
"likely", "probably", "appears to be", or "suggests".

2. UNKNOWN MEDICATION SAFE HARBOR:
If a medication’s purpose or class is unclear,
it MUST be APPROVED unless:
- the diagnosis explicitly contradicts its typical use, OR
- it duplicates another ordered medication by name, OR
- an explicit contraindication is documented

3. DO NOT REJECT MEDICATIONS FOR:
- Treatment sequencing or optimization
- Dose considerations
- Absence of laboratory values
- Prior therapy assumptions
- Clinical guideline preferences

--------------------------------------------------
WITHIN-VISIT CONSISTENCY (MANDATORY)
--------------------------------------------------

If the same service appears multiple times within the same visit
with the same diagnoses and context,
it MUST receive the SAME decision each time.
Do NOT reassess or reinterpret its appropriateness.

--------------------------------------------------
LABS, IMAGING, AND PROCEDURES
--------------------------------------------------

For non-medication services:
- Reject only when clearly unrelated, duplicated, or unnecessary
based on the provided diagnoses
- If another ordered service already serves the same purpose,
state that explicitly in the rejection reason

--------------------------------------------------
REJECTION OUTPUT RULES
--------------------------------------------------

- Return ONLY rejected services
- Do NOT mention approved services
- Each rejected service must have its own key-value entry
- Use a friendly, advisory tone to help documentation or ordering

--------------------------------------------------
OUTPUT FORMAT (STRICT)
--------------------------------------------------

Return ONLY a raw JSON object in this exact structure:

{
  "Rejected": {
    "SERVICE_CODE": "Clear rejection reason and documentation or alternative advice"
  }
}

Rules:
- Response MUST start with { and end with }
- Do NOT wrap output in markdown
- Do NOT include any text outside the JSON

--------------------------------------------------
FINAL SAFETY CHECK (MANDATORY)
--------------------------------------------------

Before returning the JSON:
- Confirm every listed service is explicitly being REJECTED
- If a rejection is based on uncertainty, ambiguity, or inference,
REMOVE it immediately
- If any value contains phrases like:
  "approved", "medically necessary", "indicated", or speculative wording,
REMOVE that entry

If no services meet rejection criteria, return:

{
  "Rejected": {}
}
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

# --- YOUR ORIGINAL FUNCTIONS ---

def dev_response(info, services, model="accounts/fireworks/models/deepseek-v3p2"):
    """
    Makes predictions on all services in a visit.
    """
    try:
        # NOTE: We create the model instance inside the function to ensure fresh connection per request
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
            if hasattr(chunk, "content") and chunk.content:
                full_response += chunk.content
        end = time.time()
        elapsed = end - start

        return elapsed, full_response
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

def clean_llm_json(response: str) -> dict:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", response.strip(), flags=re.MULTILINE)
    return cleaned

def validate_outcome(rejected):
    if rejected:
        to_del = []
        for key, value in rejected.items():
            if bool(re.search(r'\bapproved\b', str(value), re.IGNORECASE)):
                to_del.append(key)
        for key in to_del:
            del rejected[key]
    return rejected


# --- FASTAPI WRAPPER (THE NEW PART) ---

class ServiceItem(BaseModel):
    Service_Name: str
    Quantity: int

class ValidationRequest(BaseModel):
    patient_info: Dict[str, Any] = Field(default={
        "AGE": 30,
        "PATIENT_GENDER": "Male",
        "DIAGNOS_NAME": "Influenza",
        "Symptoms": "Fever, Cough"
    })
    ordered_services: Dict[str, ServiceItem] = Field(default={
        "101": {"Service_Name": "Panadol", "Quantity": 1},
        "102": {"Service_Name": "MRI Brain", "Quantity": 1}
    })

@app.post("/validate_visit")
async def validate_visit(data: ValidationRequest):
    """
    Simulates the 'request_loop' for a single visit via API.
    """
    info = data.patient_info
    services = {k: v.dict() for k, v in data.ordered_services.items()}
    
    logger.info(f"Received request for {len(services)} services")

    try:
        # 1. Get LLM Response
        res_time, answer = dev_response(info, services)

        # 2. Parse JSON (Simulating your try/except block)
        try:
            parsed = json.loads(answer)
            rejected = validate_outcome(parsed.get("Rejected", {}))
        except json.JSONDecodeError:
            try:
                cleaned = clean_llm_json(answer)
                parsed = json.loads(cleaned)
                rejected = validate_outcome(parsed.get("Rejected", {}))
            except Exception as e:
                return {"status": "error", "message": "JSON Parse Failed", "raw": answer}

        # 3. Validate Keys
        final_rejected = validate_keys(rejected)

        return {
            "status": "success",
            "inference_time": res_time,
            "medical_prediction": final_rejected,
            "raw_response": answer
        }

    except Exception as e:
        logger.error(f"Server Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)