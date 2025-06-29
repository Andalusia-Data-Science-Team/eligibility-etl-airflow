WITH
    VISIT_CTE AS (
        SELECT DISTINCT VS.ID
        FROM VISITMGT.VISITSERVICE VS
        LEFT JOIN VisitMgt.VisitFinincailInfo AS VFI ON VFI.VisitID = VS.VisitID
        WHERE VS.ClaimDate >= DATEADD(HOUR, -4, GETDATE())
          AND VS.IsDeleted = 0
          AND VS.CompanyShare > 0
          AND VFI.ContractTypeID = 1
    ),
    ChiefComplaint_CTE AS (
        SELECT DISTINCT VS.ID
        FROM PatPrlm.ChiefComplaint CC
        LEFT JOIN VISITMGT.VISITSERVICE VS ON CC.VisitID = VS.VisitID
        WHERE ISNULL(CC.UpdatedDate, CC.CreatedDate) >= DATEADD(HOUR, -4, GETDATE())
          AND CC.IsDeleted = 0
    ),
    Diagnose_CTE AS (
        SELECT DISTINCT VS.ID
        FROM Patprlm.ProblemCard AS PC
        LEFT JOIN Patprlm.ProblemCardDetail PCD ON PC.ID = PCD.ProblemCardID
        LEFT JOIN VISITMGT.VISITSERVICE VS ON PC.VisitID = VS.VisitID
        WHERE (ISNULL(PCD.UpdatedDate, PCD.CreatedDate) >= DATEADD(HOUR, -4, GETDATE())
               OR ISNULL(PC.UpdatedDate, PC.CreatedDate) >= DATEADD(HOUR, -4, GETDATE()))
          AND PC.IsDeleted = 0
          AND PCD.IsDeleted = 0
    ),
    Symptom_CTE AS (
        SELECT DISTINCT VS.ID
        FROM [PatPrlm].[SignsAndSymptoms] CC
        LEFT JOIN VISITMGT.VISITSERVICE VS ON CC.VisitID = VS.VisitID
        WHERE ISNULL(CC.UpdatedDate, CC.Createddate) >= DATEADD(HOUR, -4, GETDATE())
          AND CC.IsDeleted = 0
    ),
    CTE AS (
        SELECT ID FROM VISIT_CTE
        UNION
        SELECT ID FROM ChiefComplaint_CTE
        UNION
        SELECT ID FROM Diagnose_CTE
        UNION
        SELECT ID FROM Symptom_CTE
    )
SELECT DISTINCT

    -- VISIT Details
    VS.VisitID,
    VC.EnName AS Visit_Type,

    -- Doctor/Department Details
    V.MainSpecialityEnName AS Provider_Department,

    -- Patient Details
    G.EnName AS Patient_Gender,
    DATEDIFF(YEAR, PA.DateOfBirth, GETDATE()) AS Age,

    -- Service Details
    VS.claimdate AS Creation_Date,
    VS.UpdatedDate AS Updated_Date,
    VS.ServiceEnName AS [Description],
    VS.ID AS VisitServiceID,
    VS.Quantity,

    -- Diagnosis Details
    PCD.ICDDiagnoseNames AS Diagnose,
    PCD.ICDDiagnoseCodes AS ICD10,
    PCD.ProblemNote,

    -- ChiefComplain & Symptoms
    CC.ChiefComplaintNotes AS Chief_Complaint,
    SAS.Symptoms

FROM VisitMgt.VisitService AS VS
LEFT JOIN VisitMgt.Visit AS V ON VS.VisitID = V.ID
LEFT JOIN VISITMGT.SLKP_visitclassification VC ON V.VisitClassificationID = VC.ID
LEFT JOIN MPI.Patient PA ON PA.ID = V.PatientID
LEFT JOIN MPI.SLKP_Gender G ON PA.GenderID = G.ID
LEFT JOIN VisitMgt.VisitFinincailInfo AS VFI ON VFI.VisitID = VS.VisitID
LEFT JOIN PatPrlm.ChiefComplaint CC ON CC.VisitID = VS.VisitID
LEFT JOIN (
    SELECT
        VISITID,
        STRING_AGG(PCD.ICDDiagnoseName, ' , ') AS ICDDiagnoseNames,
        STRING_AGG(PCD.ICDDiagnoseCode, ' , ') AS ICDDiagnoseCodes,
        STRING_AGG(PC.Note, ' , ') AS ProblemNote
    FROM Patprlm.ProblemCard AS PC
    LEFT JOIN Patprlm.ProblemCardDetail PCD ON PC.ID = PCD.ProblemCardID
    GROUP BY VISITID
) AS PCD ON PCD.VisitID = VS.VisitID
LEFT JOIN (
    SELECT
        VISITID,
        STRING_AGG(SS.SignAndSymptomNotes, ',') AS Symptoms
    FROM [PatPrlm].[SignsAndSymptoms] SS
    GROUP BY VISITID
) AS SAS ON VS.VISITID = SAS.VISITID
WHERE V.VisitStatusID != 3
  AND VC.EnName != 'Ambulatory'
  AND VS.IsDeleted = 0
  AND VS.CompanyShare > 0
  AND VFI.ContractTypeID = 1
  AND VS.ID IN (
      SELECT DISTINCT ID FROM CTE
  );