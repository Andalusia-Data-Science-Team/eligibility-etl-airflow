----------------------------------------------------------------------------------------------------------------------------------------------------------------------
WITH drug_interaction AS(
SELECT DISTINCT ClaimTransactionID
FROM Nphies.ClaimItem C
WHERE  (ResponseReason LIKE '% drug interaction %' )  

  AND CONVERT(DATE,ISNULL(UpdatedDate,CreatedDate)) = CONVERT(DATE,GETDATE()-1) --FOR YESTERDAY
  --AND CONVERT(DATE, ISNULL(UpdatedDate, CreatedDate)) = CONVERT(DATE, GETDATE()) -- For today
 --AND C.UpdatedBy !='123456789'  

),
Other_MN1 AS(
SELECT DISTINCT ID
FROM Nphies.ClaimItem C
WHERE    ( ResponseReasonCode LIKE '%MN-1%' 
		OR ResponseReasonCode LIKE '%AD-1-4%'   
		OR ResponseReasonCode LIKE '%AD-3-5%'
		OR ResponseReason LIKE '%Diagnosis is inconsistent with %' 
		OR ResponseReason LIKE '%MN-1-%'
	    AND  (ResponseReason NOT LIKE '% drug interaction %') )
  AND CONVERT(DATE,ISNULL(UpdatedDate,CreatedDate)) = CONVERT(DATE,GETDATE()-1) --FOR YESTERDAY
  --AND CONVERT(DATE, ISNULL(UpdatedDate, CreatedDate)) = CONVERT(DATE, GETDATE()) -- For today
   --AND C.UpdatedBy !='123456789'

)
SELECT DISTINCT
    ---------------------------------------------------------Request Claim ----------------------------------------------------------
    REQ.RequestTransactionID,
    REQ.VisitID,
      V.CreatedDate AS VisitStartDate,
	  V.UpdatedDate,
    REQ.StatementId,
    REQ.PatientEnGender AS Gender,
    DATEDIFF(YEAR, REQ.PatientDateOfBirth, GETDATE()) AS Age,
    REQ.ContractorEnName,
    REQ.VisitClassificationEnName,
    ---------------------------------------------------------Claim Services----------------------------------------------------------
    I.Sequence,
    I.ItemId AS Service_id,
    I.NameEn AS Service_Name,
    I.ResponseState AS Status,
    I.ResponseProcessNote AS Note,
    I.ResponseReason AS Reason,
    I.ResponseReasonCode,
	I.ID AS VisitServiceID,
    ---------------------------------------------------------Diagnosis Details----------------------------------------------------------
    PCD.ICDDiagnoseNames AS Diagnosis,
    PCD.ICDDiagnoseCodes AS ICD10,
    PCD.ProblemNote,
    ----------------------------------------------------------ChiefComplain & Symptoms ---------------------------------------------------------
    CC.ChiefComplaintNotes AS Chief_Complaint,
    SAS.Symptoms

FROM (
    -- Subquery to get the most recent request for each VisitID/StatementID combination
    SELECT
        ID AS RequestTransactionID,
        VisitID,
        StatementId,
        DENSE_RANK() OVER (PARTITION BY VisitID, StatementID ORDER BY CreatedDate DESC) AS RN,
        PatientEnGender,
        PatientDateOfBirth,
        ContractorEnName,
        VisitClassificationEnName
    FROM Nphies.ClaimTransaction
    WHERE TransactionType = 'REQUEST'
    AND ClaimTransactionId IN (3, 4)  
) REQ

-- Join to Claim Items
LEFT JOIN Nphies.ClaimItem I
    ON I.ClaimTransactionID = REQ.RequestTransactionID

INNER JOIN Other_MN1 M
      ON M.ID=I.ID

-- Join to aggregated Diagnosis information
LEFT JOIN (
    SELECT
        VISITID,
        STRING_AGG(PCD.ICDDiagnoseName, ' , ') AS ICDDiagnoseNames,
        STRING_AGG(PCD.ICDDiagnoseCode, ' , ') AS ICDDiagnoseCodes,
        STRING_AGG(PC.note, ' , ') AS ProblemNote
    FROM Patprlm.ProblemCard AS PC
    LEFT JOIN Patprlm.ProblemCardDetail PCD
        ON PC.ID = PCD.ProblemCardID
    WHERE PC.IsDeleted = 0
    AND PCD.IsDeleted = 0
    GROUP BY VISITID
) AS PCD
    ON PCD.VisitID = REQ.VisitId

-- Join to Chief Complaint
LEFT JOIN PatPrlm.ChiefComplaint CC
    ON CC.VisitID = REQ.VisitID

-- Join to aggregated Symptoms
LEFT JOIN (
    SELECT
        VISITID,
        STRING_AGG(SS.SignAndSymptomNotes, ',') AS Symptoms
    FROM [PatPrlm].[SignsAndSymptoms] SS
    GROUP BY VISITID
) AS SAS
    ON REQ.VISITID = SAS.VISITID
-- VISIT START DATE
LEFT JOIN VisitMgt.Visit V
      ON V.ID=REQ.VisitID

-- Filter to only include the most recent request for each VisitID/StatementID
WHERE REQ.RN = 1
  --AND REQ.VisitId IN(500009, 502520, 554552, 556173)-- Optional filter for specific visit
  --AND ResponseReasonCode = 'MN-1-1'  -- Optional filter for specific rejection code
  --AND (ResponseReason not LIKE '% drug interaction %' OR ResponseReason not LIKE '% Drug combination %' )  


UNION ALL

SELECT DISTINCT
    ---------------------------------------------------------Request Claim ----------------------------------------------------------
    REQ.RequestTransactionID,
    REQ.VisitID,
      V.CreatedDate AS VisitStartDate,
	  V.UpdatedDate,
    REQ.StatementId,
    REQ.PatientEnGender AS Gender,
    DATEDIFF(YEAR, REQ.PatientDateOfBirth, GETDATE()) AS Age,
    REQ.ContractorEnName,
    REQ.VisitClassificationEnName,
    ---------------------------------------------------------Claim Services----------------------------------------------------------
    I.Sequence,
    I.ItemId AS Service_id,
    I.NameEn AS Service_Name,
    I.ResponseState AS Status,
    I.ResponseProcessNote AS Note,
    I.ResponseReason AS Reason,
    I.ResponseReasonCode,
	I.ID AS VisitServiceID,
    ---------------------------------------------------------Diagnosis Details----------------------------------------------------------
    PCD.ICDDiagnoseNames AS Diagnosis,
    PCD.ICDDiagnoseCodes AS ICD10,
    PCD.ProblemNote,
    ----------------------------------------------------------ChiefComplain & Symptoms ---------------------------------------------------------
    CC.ChiefComplaintNotes AS Chief_Complaint,
    SAS.Symptoms

FROM (
    -- Subquery to get the most recent request for each VisitID/StatementID combination
    SELECT
        ID AS RequestTransactionID,
        VisitID,
        StatementId,
        DENSE_RANK() OVER (PARTITION BY VisitID, StatementID ORDER BY CreatedDate DESC) AS RN,
        PatientEnGender,
        PatientDateOfBirth,
        ContractorEnName,
        VisitClassificationEnName
    FROM Nphies.ClaimTransaction
    WHERE TransactionType = 'REQUEST'
    AND ClaimTransactionId IN (3, 4)  
) REQ

-- Join to Claim Items
LEFT JOIN Nphies.ClaimItem I
    ON I.ClaimTransactionID = REQ.RequestTransactionID

INNER JOIN drug_interaction D
      ON D.ClaimTransactionID=I.ClaimTransactionID

-- Join to aggregated Diagnosis information
LEFT JOIN (
    SELECT
        VISITID,
        STRING_AGG(PCD.ICDDiagnoseName, ' , ') AS ICDDiagnoseNames,
        STRING_AGG(PCD.ICDDiagnoseCode, ' , ') AS ICDDiagnoseCodes,
        STRING_AGG(PC.note, ' , ') AS ProblemNote
    FROM Patprlm.ProblemCard AS PC
    LEFT JOIN Patprlm.ProblemCardDetail PCD
        ON PC.ID = PCD.ProblemCardID
    WHERE PC.IsDeleted = 0
    AND PCD.IsDeleted = 0
    GROUP BY VISITID
) AS PCD
    ON PCD.VisitID = REQ.VisitId

-- Join to Chief Complaint
LEFT JOIN PatPrlm.ChiefComplaint CC
    ON CC.VisitID = REQ.VisitID

-- Join to aggregated Symptoms
LEFT JOIN (
    SELECT
        VISITID,
        STRING_AGG(SS.SignAndSymptomNotes, ',') AS Symptoms
    FROM [PatPrlm].[SignsAndSymptoms] SS
    GROUP BY VISITID
) AS SAS
    ON REQ.VISITID = SAS.VISITID
-- VISIT START DATE
LEFT JOIN VisitMgt.Visit V
      ON V.ID=REQ.VisitID           

-- Filter to only include the most recent request for each VisitID/StatementID
WHERE REQ.RN = 1
  --AND REQ.VisitId IN(500009, 502520, 554552, 556173)-- Optional filter for specific visit
  --AND (ResponseReasonCode LIKE '%MN-1-1%' OR ResponseReasonCode LIKE '%MN-1-2%')-- Optional filter for specific rejection code
  --AND (ResponseReason LIKE '% drug interaction %' OR ResponseReason  LIKE '% Drug combination %' )

