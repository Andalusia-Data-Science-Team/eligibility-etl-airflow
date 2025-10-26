prompt = """
You are an expert member of the complaints patient experience team, you are supposed to think about the provided complaints data, validate the preventive actions
provided if exists (say whether it's relevant, irrelevant, or none was found), and recommend enhancements or recommend preventive actions if there isn't any.
Preventive actions are actions that should be taken in order to prevent this complaint or issue from recurring again, get creative but also stay realistic.
You should provide smart and professional solutions, not any generic templated solutions.
When proposing to implement a new system, provide some level of detail for this system. Pay attention to the details of the complaint, and notice whether any solution is already implemented.
Also, you are supposed to classify the complaint into one of the following categories:
['operation wrong information (scheduling/price)',
 'nurse response',
 'long waiting time',
 'doctor delay to clinic time',
 'poor communication/behavior/attitude',
 'call center delayed response',
 'insuffiecint medical examination',
 'call center delayed follow up',
 'wrong diagnosis',
 'clinic cancellation/modification',
 'medical records accuracy',
 'call center wrong reservation',
 'unsuccessful ttt outcome',
 'insufficient patient education',
 'nurse follow up',
 'noise',
 'rad delayed results',
 'lab wrong results',
 'call center wrong info',
 'lab delayed results',
 'medication delay',
 'approval delay',
 'housekeeping quality',
 'medical records delay',
 'unavailability of medication',
 'queuing system error',
 'wrong medication dispensed',
 'front office delay',
 'rad wrong results',
 'wrong medication prescribtion',
 'nurse technicality/cannula',
 'mobile application',
 'system down',
 'wrong medication adminstrated']
If the complaint belongs to more than one of the previous categories, choose the most relevant one only. If it's a thanks not a complaint classify it as 'Thanks'.

You are supposed to return your output in a valid JSON format that looks like this:
{'Validation of Preventive Actions': 'Irrelevant'
'Category': 'Long waiting time'
'Recommended Preventive Actions': 'Add your recommended actions here...'}
"""
schema = {
    "type": "object",
    "properties": {
        "Validation of Preventive Actions": {
            "type": "string",
            "description": "Validation assessment of the effectiveness and relevance of preventive actions taken"
        },
        "Category": {
            "type": "string",
            "description": "The category that the complaint belongs to from predefined categories"
        },
        "Recommended Preventive Actions": {
            "type": "string",
            "description": "Enhancements or new preventive actions to avoid recurrence of complaint"
        }
    },
    "required": ["Validation of Preventive Actions", "Category", "Recommended Preventive Actions"],
    "additionalProperties": False,
}


def data_prep(df):
    columns = [
        "Department",
        "Description",
        "Initial Investigation",
        "Initial Satisfaction",
        "Investigation ",
        "Corrective Action Taken",
        "Satisfaction Action",
        "Preventive Action Taken",
    ]
    data = df[columns].to_dict()
    return data


def call_llm(data, model="accounts/fireworks/models/deepseek-v3"):
    json_model = ChatFireworks(
        model=model, temperature=0.2, request_timeout=(120, 120)
    ).bind(response_format={"type": "json_object", "schema": schema})

    chat_history = [
        SystemMessage(content=prompt),
        HumanMessage(content=str(data)),
    ]

    response = json_model.invoke(chat_history).content
    return response