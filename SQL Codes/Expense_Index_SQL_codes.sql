/*--------EXPENSE INDEX REFRESH FOR (2015-2017Q3)----------------------------------------------------------------------------*/
/*--------1. Database taken into account are US and EMEA Cluster-------------------------------------------------------------*/
/*--------2. Time period taken into account for the analysis 2015Q1-2016Q4---------------------------------------------------*/
/*--------3. Base table preparations from "c_expense.report_entry","c_expense.report", "c_expense.employee"------------------
             c_outtask.outtask_user and c_outtask.outtask_user_derived_cols -------------------------------------------------*/
			 
CREATE TABLE U_FACTSPAN.EXPENSEINDEX_2015 as
SELECT SUBSTR(RE.trans_date_key, 0, 4) as Year
     , CONCAT('Q', case when substr(RE.trans_date_key,5,2) IN ('01','02','03') then 1				
						when substr(RE.trans_date_key,5,2) IN ('04','05','06') then 2
						when substr(RE.trans_date_key,5,2) IN ('07','08','09') then 3
						when substr(RE.trans_date_key,5,2) IN ('10','11','12') then 4 end) as quarter
						, SUBSTR(RE.trans_date_key, 5, 2) as Month
	 ,  RE.expense_type_spend_category_name                           
     ,  RE.entity_id
     ,  RE.employee_key
     ,  RE.rpt_fact_key   
	 ,  RE.rpt_entry_fact_key
	 ,  RE.country_name as Entry_Country
	 ,  RE.trans_amt_currency_name
	 ,  RE.reimbursement_currency_name
	 ,  RE.reporting_currency_name
	 ,  U.user_id
	 ,  OU.user_age
     ,  RE.approved_usd_amt
	 ,  R.country_name as report_country

FROM c_expense.report_entry RE
JOIN c_expense.report R ON (RE.rpt_fact_key = R.rpt_fact_key AND RE.entity_id = R.entity_id)
JOIN c_expense.employee E ON (E.employee_key = RE.employee_key AND E.entity_id = RE.entity_id)
JOIN c_outtask.outtask_user U ON (U.GUUID = E.CUUID)
LEFT JOIN c_outtask.outtask_user_derived_cols OU ON (OU.user_id = U.user_id)

WHERE (RE.rpt_entry_transaction_type = 'REG' OR RE.rpt_entry_transaction_type = 'CHD')
  AND (RE.approved_usd_amt < 250000 AND RE.approved_usd_amt > -250000)
  AND (RE.trans_date_key >= 20150101 AND RE.trans_date_key <= 20151231)
  AND (R.processing_payment_dttm IS NOT NULL)
  AND (UPPER(E.is_system_record_flag_name) = 'NO' AND UPPER(E.is_test_employee_name) = 'NO')
  AND UPPER(RE.is_personal_expenditure_flag_name) = 'NO'
  
  
  
/*--------BASE TABLE-----------------------------------------------------------------------------------------------------*/
/*--------Union all years data from base table extracted from 2015Q1 to 2017Q3-------------------------------------------*/

CREATE TABLE U_FACTSPAN.EXPENSEINDEX_15Q1_17Q3 as
select * from expenseindex_2015 
union all 
select * from expenseindex_2016 
union all
select * from expenseindex_2017

/*--------INDUSTRY LEVEL DATA ------------------------------------------------------------------------------------------------*/
/*--------Industry "c_expense.all_company" & "c_edw.d_client" but for EMEA cluster "entity_company"---------------------------*/
create table u_factspan.expenseindex_1517_Industry stored as parquet as
select A.*, B.industry,B.vertical,B.owner_theater,B.owner_business_unit
from U_FACTSPAN.expenseindex_15q1_17q3 A 
left join p_curated.all_company C on (A.entity_id = C.entity_id)
left join (select sfa_id_15_digit,industry,vertical,owner_theater,owner_business_unit 
from c_edw.d_client 
group by sfa_id_15_digit,industry,vertical,owner_theater,owner_business_unit ) B     
on C.salesforce_id = B.sfa_id_15_digit


/*--------Company level pull-----------------------------------------------------------------------------------------------*/
select A.year,A.quarter,
       A.entity_id,A.owner_business_unit,
       count (distinct (concat(A.entity_id,A.employee_key))) as DistinctUsers,
       count(distinct concat(A.entity_id,A.rpt_fact_key,A.rpt_entry_fact_key)) as Transactions,
       sum(A.approved_usd_amt) as Amount,
       A.company_name
       from (select C.*,B.company_name from u_factspan.expenseindex_1517_industry C 
             left join c_expense.entity_company B on 
             C.entity_id=B.entity_id) A 
       where A.year = '2017'
       group by A.year,A.quarter,A.entity_id,A.company_name,A.owner_business_unit












 