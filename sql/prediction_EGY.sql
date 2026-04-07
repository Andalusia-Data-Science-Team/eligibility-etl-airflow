SELECT 
			VisitID,
			BU,
			Visit_Type,
			Department_ID,
			Provider_Department	Patient_Gender,
			Age,
			VisitServiceID,
			Creation_Date,
			Updated_Date,
			Service_Name,	
			Service_Code,	
			Quantity,
			Diagnose,
			ICD10,	
			ProblemNote,
			Chief_Complaint,	
			Symptoms,	
			ContractorID,	
			ContractorEnName
FROM DWH_Claims.dbo.EGY_MedPred_Final
WHERE CONVERT(DATE,VisitCreatedDate ) >='2026-03-01' and 
Medical_Prediction IS NULL