CREATE OR REPLACE TABLE project_analytics.merck_prevymis.MERCK_PREVYMIS_J_CODE_CLAIMS_20180824 AS

SELECT  DISTINCT CLAIM_NUMBER
				, encrypted_key_1
				, encrypted_key_2
				, NDC
FROM    RWD_DB.RWD.RAVEN_CLAIMS_SUBMITS_PROCEDURE PROC 
        
WHERE   YEAR_OF_SERVICE BETWEEN '2017-07-01' AND '2018-08-31'
        AND UPPER(PROC.PROCEDURE) = 'J3490'

;

CREATE OR REPLACE TABLE project_analytics.merck_prevymis.MERCK_PREVYMIS_J_CODE_CLAIMS_20180824 AS

SELECT  DISTINCT CLAIM_NUMBER
				, encrypted_key_1
				, encrypted_key_2
FROM    project_analytics.merck_prevymis.MERCK_PREVYMIS_J_CODE_CLAIMS_20180824
        
WHERE   NDC IN ('00006307502','00006307504','00006307602','00006307604','00006500301','00006500401')
		OR NDC IS NULL
		OR NDC like '%NULL%'

;

CREATE OR REPLACE TABLE project_analytics.merck_prevymis.MERCK_PREVYMIS_DX_CODE_CLAIMS_20180824 AS

SELECT  DISTINCT CLAIM_NUMBER
				, encrypted_key_1
				, encrypted_key_2
FROM    RWD_DB.RWD.RAVEN_CLAIMS_SUBMITS_DIAGNOSIS DIAG
        
WHERE   YEAR_OF_SERVICE BETWEEN '2017-07-01' AND '2018-08-31'
        AND REPLACE(DIAG.DIAGNOSIS,'.','') IN ('785','V5844','B25','B250','B251','B252','B258','B259',
            'C8318','C9000','C9100','C9101','C9200','C9201','D702','T86819','Z79899','Z940','Z941',
            'Z942','Z944','Z9481','Z9484')


;

create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_patients_20180824 as
select distinct A.encrypted_key_1
				, A.encrypted_key_2
from project_analytics.merck_prevymis.MERCK_PREVYMIS_J_CODE_CLAIMS_20180824 A
		inner join project_analytics.merck_prevymis.MERCK_PREVYMIS_DX_CODE_CLAIMS_20180824 B
				on A.claim_number = B.claim_number
where A.encrypted_key_1 not like 'XXX -%' and A.encrypted_key_2 not like 'XXX -%';

				


--extract data
--for submits get all claim_number for these patients
--get Claims_submits_header
create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_claim_number_20180824 as
select distinct B.	CLAIM_NUMBER
				, B.encrypted_key_1
				, B.encrypted_key_2
from project_analytics.merck_prevymis.MERCK_PREVYMIS_patients_20180824 A
		inner join rwd_db.rwd.RAVEN_EXTERNAL_CLAIMS_SUBMITS_HEADER B
				on A.encrypted_key_1 = B.encrypted_key_1
				and A.encrypted_key_2 = B.encrypted_key_2;


-------------------------------------------------------------------------				

create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_header_20180824 as
select distinct CLAIM_NUMBER
				,	ENCRYPTED_KEY_1
				,	ENCRYPTED_KEY_2
				,	RECEIVED_DATE
				,	CLAIM_TYPE_CODE				
				,	STATEMENT_FROM
				,	STATEMENT_TO
				,	MIN_SERVICE_FROM
				,	MAX_SERVICE_TO
				,	TOTAL_CHARGE
				,	TOTAL_ALLOWED
				,	DRG_CODE
				,	TYPE_BILL
				, 	case when claim_type_code = 'I' then B.POS_description
						 when claim_type_code = 'P' then C.DESCRIPTION
						 end as POS_description
				,	ADMISSION_DATE
				,	ADMIT_TYPE_CODE
				,	ADMIT_SRC_CODE
				,	DISCHARGE_HOUR
				,	DISCHARGE_STATUS
				,	YEAR_OF_SERVICE
				,	ETL_DRG_ID
				,	ETL_CREATE_TS
				,	ETL_UPDATE_TS
from (select *
		from rwd_db.rwd.RAVEN_EXTERNAL_CLAIMS_SUBMITS_HEADER
		where claim_number in (select claim_number from project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_claim_number_20180824)
		) A
		left join project_analytics.reference.sv_bill_type_pos_xwalk B
				on B.BILL_TYPE_TWO_DIGIT =  left(A.type_bill,2)
		left join project_analytics.reference.SV_POS_DESCRITION C
				on left(A.type_bill,2) = C.POS;


				
--get Claims_submits_procedure
create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_procedure_20180824 as
select  
				A.CLAIM_NUMBER,
				ENCRYPTED_KEY_1	,
				ENCRYPTED_KEY_2	,
				PROCEDURE_TYPE	,
				PROCEDURE	,
				PROCEDURE_SEQUENCE	,
				LINE_NUMBER	,
				STATEMENT_FROM	,
				STATEMENT_TO	,
				SERVICE_FROM	,
				SERVICE_TO	,
				RECEIVED_DATE	,
				YEAR_OF_SERVICE	,
				PLACE_SERVICE	,
				PROCEDURE_QUAL	,
				PROCEDURE_MODIFIER_1	,
				PROCEDURE_MODIFIER_2	,
				PROCEDURE_MODIFIER_3	,
				PROCEDURE_MODIFIER_4	,
				LINE_CHARGE	,
				UNITS	,
				REVENUE_CODE	,
				DIAGNOSIS_POINTER_1	,
				DIAGNOSIS_POINTER_2	,
				DIAGNOSIS_POINTER_3	,
				DIAGNOSIS_POINTER_4	,
				DIAGNOSIS_POINTER_5	,
				DIAGNOSIS_POINTER_6	,
				DIAGNOSIS_POINTER_7	,
				DIAGNOSIS_POINTER_8	,
				NDC	,
				RENDERING_PROVIDER_NPI	,
				AMBULANCE_TO_HOSP	,
				EMERGENCY	,
				PAID_AMOUNT	,
				BENE_NOT_ENTITLED	,
				PATIENT_REACH_MAX	,
				SVC_DURING_POSTOP	
				  ETL_DRG_ID,
				  ETL_CREATE_TS,
				  ETL_UPDATE_TS				
from rwd_db.rwd.RAVEN_EXTERNAL_CLAIMS_SUBMITS_PROCEDURE A
where A.claim_number in (select claim_number from project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_claim_number_20180824);


				
------------------------------------------------------------------------

--get Claims_submits_diagnosis
create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_diagnosis_20180824 as
select  A.CLAIM_NUMBER,
				A.ENCRYPTED_KEY_1,
				A.ENCRYPTED_KEY_2,
				CODE_TYPE,
				HIPAA_METHOD1_DIAGNOSIS AS DIAGNOSIS,
				DIAGNOSIS_SEQUENCE	,
				STATEMENT_FROM	,
				STATEMENT_TO	,
				MIN_SERVICE_FROM	,
				MAX_SERVICE_TO	,
				RECEIVED_DATE	,
				YEAR_OF_SERVICE
				  ETL_DRG_ID,
				  ETL_CREATE_TS,
				  ETL_UPDATE_TS				
from rwd_db.rwd.RAVEN_EXTERNAL_CLAIMS_SUBMITS_DIAGNOSIS A
where A.claim_number in (select claim_number from project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_claim_number_20180824);

				
--get Claims_submits_payer
create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_payer_20180824 as
select  A.CLAIM_NUMBER	,
				A.ENCRYPTED_KEY_1	,
				A.ENCRYPTED_KEY_2	,
				HIPAA_METHOD1_PAYER_ID AS PAYER_ID	,
				HIPAA_METHOD1_PAYER_NAME AS PAYER_NAME	,
				PAYER_STATE	,
				TYPE_COVERAGE	,
				PAYER_SEQUENCE	,
				PROCESS_DATE	,
				YEAR_OF_SERVICE
				  ETL_DRG_ID,
				  ETL_CREATE_TS,
				  ETL_UPDATE_TS				
from rwd_db.rwd.RAVEN_EXTERNAL_CLAIMS_SUBMITS_payer A
where A.claim_number in (select claim_number from project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_claim_number_20180824);	


--get Claims_submits_patient
create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_patient_20180824 as
select   A.CLAIM_NUMBER	,
				MEMBER_DOB	,
				MEMBER_ADR_STATE	,
				MEMBER_ADR_ZIP	,
				MEMBER_ENCRYPTED_KEY_1	,
				MEMBER_ENCRYPTED_KEY_2	,
				PATIENT_RELATION	,
				PATIENT_GENDER	,
				PATIENT_DOB	,
				ENCRYPTED_KEY_1	,
				ENCRYPTED_KEY_2	,
				KEY3	,
				PAYER_SEQUENCE	,
				YEAR_OF_SERVICE
				  ETL_DRG_ID,
				  ETL_CREATE_TS,
				  ETL_UPDATE_TS				
from  rwd_db.rwd.RAVEN_EXTERNAL_CLAIMS_SUBMITS_patient A
where A.claim_number in (select claim_number from project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_claim_number_20180824);	


--get Claims_submits_provider
create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_provider_20180824 as
select  A.CLAIM_NUMBER	,
				A.ENCRYPTED_KEY_1	,
				A.ENCRYPTED_KEY_2	,
				PROVIDER_TYPE	,
				HIPAA_METHOD1_PROVIDER_NPI AS PROVIDER_NPI	,
				ENTITY_TYPE_CODE	,
				HIPAA_METHOD1_OTHER_PROVIDER_ID AS OTHER_PROVIDER_ID	,
				HIPAA_METHOD1_PROVIDER_UPIN AS PROVIDER_UPIN	,
				HIPAA_METHOD1_PROVIDER_STATE_LIC AS PROVIDER_STATE_LIC	,
				HIPAA_METHOD1_PROVIDER_NAME1 AS PROVIDER_NAME1	,
				HIPAA_METHOD1_PROVIDER_NAME2 AS PROVIDER_NAME2	,
				HIPAA_METHOD1_ADR_LINE1 AS ADR_LINE1	,
				HIPAA_METHOD1_ADR_LINE2 AS ADR_LINE2	,
				ADR_CITY	,
				ADR_STATE	,
				ADR_ZIP	,
				CLAIM_SOURCE_SPECIALTY_TAXONOMY	,
				YEAR_OF_SERVICE	
				  ETL_DRG_ID,
				  ETL_CREATE_TS,
				  ETL_UPDATE_TS
from rwd_db.rwd.RAVEN_EXTERNAL_CLAIMS_SUBMITS_PROVIDER A
where A.claim_number in (select claim_number from project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_claim_number_20180824);





alter table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_header_20180824
			alter encrypted_key_1 varchar(300)
			, encrypted_key_2 varchar(300);
									
update project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_header_20180824 ori
	set ori.claim_number = sha2(ori.claim_number, 256)
		, ori.encrypted_key_1 = sha2(ori.encrypted_key_1, 256)
		, ori.encrypted_key_2 = sha2(ori.encrypted_key_2, 256);
		
		
		

			
update project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_procedure_20180824 ori
	set ori.claim_number = sha2(ori.claim_number, 256)
		, ori.encrypted_key_1 = sha2(ori.encrypted_key_1, 256)
		, ori.encrypted_key_2 = sha2(ori.encrypted_key_2, 256);			
			



update project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_diagnosis_20180824 ori
	set ori.claim_number = sha2(ori.claim_number, 256)
		, ori.encrypted_key_1 = sha2(ori.encrypted_key_1, 256)
		, ori.encrypted_key_2 = sha2(ori.encrypted_key_2, 256);		




update project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_payer_20180824 ori
	set ori.claim_number = sha2(ori.claim_number, 256)
		, ori.encrypted_key_1 = sha2(ori.encrypted_key_1, 256)
		, ori.encrypted_key_2 = sha2(ori.encrypted_key_2, 256);				
		

update project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_patient_20180824 ori
	set ori.claim_number = sha2(ori.claim_number, 256)
		, ori.encrypted_key_1 = sha2(ori.encrypted_key_1, 256)
		, ori.encrypted_key_2 = sha2(ori.encrypted_key_2, 256);		
        

update project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_patient_20180824 ori
	set ori.member_encrypted_key_1 = sha2(ori.member_encrypted_key_1, 256)
		, ori.member_encrypted_key_2 = sha2(ori.member_encrypted_key_2, 256);	
        
		
update project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_provider_20180824 ori
	set ori.claim_number = sha2(ori.claim_number, 256)
		, ori.encrypted_key_1 = sha2(ori.encrypted_key_1, 256)
		, ori.encrypted_key_2 = sha2(ori.encrypted_key_2, 256);				
		

create or replace table project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_diagnosis_20180824_rownum as
select a.*, rownumber() over( partition by claim_number order by claim_number,diagnosis_sequence)  from 
project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_diagnosis_20180824 a
limit 10
---------------------------------Next Month Code to be added--------------------------------------------------------------
CREATE OR REPLACE TABLE project_analytics.merck_prevymis.MERCK_PREVYMIS_DX_CODE_CLAIMS_20180727 AS

SELECT DISTINCT CLAIM_NUMBER
                , encrypted_key_1
                , encrypted_key_2
FROM    project_analytics.merck_prevymis.MERCK_PREVYMIS_DX_CODE_CLAIMS_20180727

UNION

SELECT DISTINCT CLAIM_NUMBER
                , encrypted_key_1
                , encrypted_key_2

FROM    RWD_DB.RWD.RAVEN_CLAIMS_SUBMITS_PROCEDURE

WHERE    NDC IN ('00006307502','00006307504','00006307602','00006307604','00006500301','00006500401')
-------------------------------------------------------------------------------------------------------------------------
		
		
grant all on project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_header_20180824 TO RWD_ANALYTICS_RW;
grant all on MERCK_PREVYMIS_submits_procedure_20180824 TO RWD_ANALYTICS_RW;
grant all on project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_diagnosis_20180824 TO RWD_ANALYTICS_RW;
grant all on project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_payer_20180824 TO RWD_ANALYTICS_RW;
grant all on project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_patient_20180824 TO RWD_ANALYTICS_RW;
grant all on project_analytics.merck_prevymis.MERCK_PREVYMIS_submits_provider_20180824 TO RWD_ANALYTICS_RW;
