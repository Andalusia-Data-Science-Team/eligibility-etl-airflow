SELECT DISTINCT
	CS.VISITID as VisitID,
	CS.PatientID,
	EnDisplayName AS Form_Type,
	CS.HTMLBody,
	CN.Body

FROM CS.PatientClinicalSheet CS
LEFT JOIN CS.FORM F
ON CS.FormID=F.ID
LEFT JOIN CN.ClinicalNote CN
ON CN.VisitID = CS.VisitID
WHERE CS.[StatusID] !=3 AND CS.ISDELETED=0
AND CS.SignDate >= DATEADD(DAY, -1, CURRENT_TIMESTAMP)
AND CS.VisitClassificationEnName = 'Outpatient'