SELECT DISTINCT
	FB.Episode_key,
	ds.Job_Desc as specialty,
	FB.patient_id,
	CN.Note,
	NT.EnName AS NotesType,
	VS.Value1 AS Vital_Sign1,
	VST.EnName AS Vital_SignType,
	DD.DiagnoseName,
	DD.ICDDiagnoseCode,
	P.Age,
	P.Gender_Description AS Gender,
	DCS.ChiefComplaintNotes,
	DCS.SymptomNotes

FROM DAX.FactBilling FB

LEFT JOIN dax.DimClinicalNotes CN
ON CN.Episode_Key = FB.Episode_key
LEFT JOIN [dax].[DimNotesType] NT
ON CN.TypeID = NT.ID
LEFT JOIN dax.Dim_VitalSign VS
ON FB.Episode_key = VS.Episode_Key 
LEFT JOIN dax.Dim_VitalSign_Type VST
ON VS.VitalSignType_ID = VST.VitalSignType_ID
LEFT JOIN dax.DimDiagnosisN DD
ON FB.Episode_key = DD.Episode_Key
LEFT JOIN DAX.DimPatient P 
ON FB.patient_id = P.PATIENT_ID
LEFT JOIN DAX.DimChiefComplaint_Symptoms DCS
ON FB.Episode_key = DCS.VisitID
LEFT JOIN dax.DimPatientEpisodeType PT 
ON fb.PatientEpisodeType_Key=pt.PatientEpisodeType_Key
LEFT JOIN dax.DimStaff ds 
ON fb.Doctor_Key=ds.Staff_Key
WHERE FB.Organization_Key = 1 
AND FB.Service_Date >= DATEADD(DAY, -1, CURRENT_TIMESTAMP)
AND pt.PatientEpisodeType_Desc = 'Outpatient'
