WITH RejectedItems AS
(
    SELECT
        C.ID AS ClaimItemID,
        C.ClaimTransactionID,
        C.ResponseReasonCode,
        C.ResponseReason,
 
        CASE
            WHEN C.ResponseReasonCode IN
            (
                'CV-3-2',
                'CV-4-8',
                'CV-4-1',
                'CV-4-2'
            )
            THEN 'Duplicated Services'
 
            WHEN C.ResponseReasonCode IN
            (
                'CV-1-3',
                'MN-1-1',
                'CV-4-9',
                'CV-1-1',
                'CV-1-2',
                'AD-3-5',
                'AD-3-6',
                'AD-3-7',
                'AD-3-8',
                'AD-1-1',
                'AD-1-2',
                'AD-1-3',
                'AD-1-4',
                'AD-1-5',
                'AD-1-6',
                'AD-1-7',
                'CV-4-7'
            )
            THEN 'Medical Necessity'
        END AS JustificationType
 
    FROM Nphies.ClaimItem C
 
    WHERE 
	ISNULL(C.UpdatedDate, C.CreatedDate) >=
              CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
 
      AND ISNULL(C.UpdatedDate, C.CreatedDate) <
              CONVERT(DATE, GETDATE()) AND
 
       C.ResponseReasonCode IN
      (
          'CV-1-3',
          'MN-1-1',
          'CV-4-9',
          'CV-1-1',
          'CV-1-2',
          'AD-3-5',
          'AD-3-6',
          'AD-3-7',
          'AD-3-8',
          'AD-1-1',
          'AD-1-2',
          'AD-1-3',
          'AD-1-4',
          'AD-1-5',
          'AD-1-6',
          'AD-1-7',
          'CV-4-7',
          'CV-3-2',
          'CV-4-8',
          'CV-4-1',
          'CV-4-2'
      )
),
LatestRequest AS
(
    SELECT
        CT.ID AS RequestTransactionID,
        CT.VisitID,
        CT.StatementID,
        CT.PatientEnGender,
        CT.PatientDateOfBirth,
        CT.ContractorEnName,
        CT.VisitClassificationEnName,
 
        ROW_NUMBER() OVER
        (
            PARTITION BY CT.VisitID, CT.StatementID
            ORDER BY CT.CreatedDate DESC, CT.ID DESC
        ) AS RN
 
    FROM Nphies.ClaimTransaction CT
 
    WHERE CT.TransactionType = 'REQUEST'
)
SELECT DISTINCT TOP 100
    REQ.RequestTransactionID,
    REQ.VisitID,
    REQ.StatementId,
    V.CreatedDate AS VisitStartDate,
    V.UpdatedDate AS VisitUpdatedDate,
    REQ.StatementID,
 
    CASE
        WHEN REQ.VisitClassificationEnName IN
             ('OPD', 'Outpatient', 'Ambulatory')
            THEN 'OPD'
 
        WHEN REQ.VisitClassificationEnName IN
             ('IP', 'IPD', 'Inpatient')
            THEN 'IP'
 
        WHEN REQ.VisitClassificationEnName IN
             ('ER', 'Emergency')
            THEN 'ER'
 
        ELSE REQ.VisitClassificationEnName
    END AS MedicalDepartment,
 
    REQ.PatientEnGender AS Gender,
 
    DATEDIFF
    (
        YEAR,
        REQ.PatientDateOfBirth,
        GETDATE()
    )
    -
    CASE
        WHEN DATEADD
        (
            YEAR,
            DATEDIFF(YEAR, REQ.PatientDateOfBirth, GETDATE()),
            REQ.PatientDateOfBirth
        ) > GETDATE()
        THEN 1
        ELSE 0
    END AS Age,
 
    REQ.ContractorEnName,
    REQ.VisitClassificationEnName,
 
    I.Sequence,
    I.ItemID AS ServiceID,
    I.NameEn AS ServiceName,
    I.ResponseState AS Status,
    I.ResponseProcessNote AS Note,
    I.ResponseReason AS RejectionReason,
    I.ResponseReasonCode,
    I.ID AS ClaimItemID,
 
    R.JustificationType,
 
    PCD.ICDDiagnoseNames AS Diagnosis,
    PCD.ICDDiagnoseCodes AS ICD10,
    PCD.ProblemNote,
 
    CC.ChiefComplaintNotes AS ChiefComplaint,
    SAS.Symptoms
 
FROM LatestRequest REQ
 
INNER JOIN Nphies.ClaimItem I
    ON I.ClaimTransactionID = REQ.RequestTransactionID
 
INNER JOIN RejectedItems R
    ON R.ClaimItemID = I.ID
 
LEFT JOIN
(
    SELECT
        PC.VisitID,
 
        STRING_AGG(PCD.ICDDiagnoseName, ' , ')
            AS ICDDiagnoseNames,
 
        STRING_AGG(PCD.ICDDiagnoseCode, ' , ')
            AS ICDDiagnoseCodes,
 
        STRING_AGG(PC.Note, ' , ')
            AS ProblemNote
 
    FROM PatPrlm.ProblemCard PC
 
    INNER JOIN PatPrlm.ProblemCardDetail PCD
        ON PCD.ProblemCardID = PC.ID
       AND PCD.IsDeleted = 0
 
    WHERE PC.IsDeleted = 0
 
    GROUP BY PC.VisitID
) PCD
    ON PCD.VisitID = REQ.VisitID
 
LEFT JOIN PatPrlm.ChiefComplaint CC
    ON CC.VisitID = REQ.VisitID
 
LEFT JOIN
(
    SELECT
        VisitID,
 
        STRING_AGG(SignAndSymptomNotes, ' , ')
            AS Symptoms
 
    FROM PatPrlm.SignsAndSymptoms
 
    GROUP BY VisitID
) SAS
    ON SAS.VisitID = REQ.VisitID
 
LEFT JOIN VisitMgt.Visit V
    ON V.ID = REQ.VisitID
INNER JOIN PMGT.PRODUCT PT
	ON I.ItemID=PT.ID
 
WHERE REQ.RN = 1
  --AND REQ.VisitId IN(500009)-- Optional filter for specific visit
  AND I.ResponseState !='approved'
  AND ISNULL(I.NameEn, '') NOT LIKE '%Package%'
  AND ISNULL(I.NameEn, '') NOT LIKE '%Accom%'
  AND PT.Code NOT LIKE 'AC%'
  AND PT.Code NOT LIKE 'PK%'