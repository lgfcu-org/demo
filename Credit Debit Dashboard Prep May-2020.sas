%INCLUDE '\\SASAPPEBI\SASSHARE1\Projects\BusinessAnalytics\Common\PROGRAMS\LIBNAMES.SAS' ;
 libname cardanal '\\SASAPPEBI\SASSHARE1\Datasets\BusinessAnalytics\murali.sastry';
libname nestor '\\SASAPPEBI\SASSHARE1\Datasets\BusinessAnalytics\michael.nestor';
libname lpeel '\\SASAPPEBI\SASSHARE1\Datasets\BusinessAnalytics\leigh.peel';
DATA WORK.MEMBER;
	SET ANALYTDM.MASTER_MEMBER (WHERE=(CURRENTRECORD = 'Y'));
RUN;
*/Derivation of Card Summary Data from Visa Card Transactions Data /*;
proc sql;
 create table work.cardsummarydata as
 Select distinct
 MonthendDate as Monthend_Date
 ,intnx('Month',MonthendDate,0,'same') as Month_n format=yymmn6.
 ,intnx('Quarter',Monthend_Date,0,'same') as Quarter_n format=yyQ6.
 ,AccountNumeric as Card_Num
 ,SSNPrimaryAccountHolder as SSN
 ,SSNSecondaryAccountHolder
 ,CreditDebit as Credit_Debit
 ,Payment_Method 
 ,Purchase_Method
 ,(sum(Trans_Cnt)) as Trans_Cnt format=comma23.
 ,(sum(Trans_Amt)) as Trans_Amt format=dollar23.2
,(sum(Issuer_Fee_Amt)) as IntChgFee_Income format=dollar23.2
from analgp.VVO_CARDTRANSACTIONS 
where MonthendDate le '31Mar2020'd
Group by MonthendDate, AccountNumeric, SSNPrimaryAccountHOlder, CreditDebit, Payment_Method, Purchase_Method
order by MonthendDate, AccountNumeric, SSNPrimaryAccountHolder, CreditDebit, Payment_Method, Purchase_Method
;
Quit;
/*Addition of Credit Score, Credit Score Range, Age, and Age Range */
PROC SQL;
	Create Table WORK.CardSumData1 as
	Select 
	t1.Month_n
	,t1.Monthend_Date
	,t1.Credit_Debit
	,t1.Quarter_n
	,t1.Card_Num
	,t1.Purchase_Method
	,t1.Payment_Method
	,t1.Trans_Cnt
	,t1.Trans_Amt
	,t1.IntChgFee_Income
	,t1.SSN
	,t1.SSNSecondaryaccountholder as SSNS_Credit
	,t2.MemberAddressCity
	,t2.MemberAddressState
	,t2.MemberAddressZipCode
	,t2.MemberAddressLatitude
	,t2.MemberAddressLongitude
	,t2.BirthDate
	,t2.Gender
	,t2.MaritalStatus
	,t2.BeaconScore,
	CASE 
		WHEN BeaconScore BETWEEN 0 and 579 THEN '<=579'
		WHEN BeaconScore BETWEEN 580 and 669 THEN '580-669'
		WHEN BeaconScore BETWEEN 670 and 739 THEN '670-739'
		WHEN BeaconScore BETWEEN 740 and 799 THEN '740-799'
		WHEN BeaconScore GE 800 THEN '>=800'
		ELSE 'Missing'
		END AS CredScr_Range
			,Yrdif(t2.BirthDate,t1.Monthend_Date,'ACT/365') as AGE format=4.1,
	CASE 
		WHEN Calculated AGE BETWEEN 0 AND 18 THEN '0-18'
		WHEN Calculated AGE GT 18 AND Calculated AGE LE 25 THEN '19-25'
		WHEN Calculated AGE GT 25 AND Calculated AGE LE 35 THEN '26-35'
		WHEN Calculated AGE GT 35 AND Calculated AGE LE 45 THEN '36-45'
		WHEN Calculated AGE GT 45 AND Calculated AGE LE 55 THEN '46-55'
		WHEN Calculated AGE GT 55 AND Calculated AGE LE 65 THEN '56-65'
		WHEN Calculated AGE GT 65 AND Calculated AGE LE 75 THEN '66-75'
		WHEN Calculated AGE GT 75 THEN '75+'
		ELSE 'Missing'
		END AS AGE_RANGE
		FROM work.cardsummarydata t1
		left join work.member t2 on (t1.SSN=t2.SSN);
	Quit;
	/* Saving in autoload folder for creating dashboard/visuals in SAS Visual Analytics 7.4 */;
data autoload.cardsummarydata_musa;
	set work.cardsumdata1;
	run;
	/*Derivation of Market Summary Data from Visa Card Transactions Data */;
 proc sql;
 create table work.marketsummaryOne as
 Select distinct
 MonthendDate as Monthend_Date
 ,intnx('Month',MonthendDate,0,'same') as Month_n format=yymmn6.
 ,CreditDebit as Account_Funding_Source
 ,Payment_Method
 ,Purchase_Method
 ,Market_Segment
 ,Merchant_DBA
 ,Merchant_ZipCode
/* ,ZIPCITY(Merchant_ZipCode) as Merchant_City*/
/* ,ZIPSTATE(Merchant_ZipCode) as Merchant_State*/
 ,(sum(Trans_Cnt)) as Trans_Cnt format=comma23.
 ,(sum(Trans_Amt)) as Trans_Amt format=dollar23.2
,(sum(Issuer_Fee_Amt)) as IntChgFee_Income format=dollar23.2
from analgp.VVO_CARDTRANSACTIONS 
where MonthendDate le '31Mar2020'd
Group by MonthendDate, Market_Segment, Merchant_DBA, Merchant_ZipCode, CreditDebit, Payment_Method, Purchase_Method, Market_Segment, Merchant_DBA, Merchant_Zipcode
order by MonthendDate, Market_Segment, Merchant_DBA, Merchant_ZipCode, CreditDebit, Payment_Method, Purchase_Method, Market_Segment, Merchant_DBA, Merchant_Zipcode
;
Quit;
/*Addition of City column from Zip Code*/;
 proc sql;
 create table work.marketsummarytwo as
 Select distinct
 Monthend_Date
 ,Month_n format=yymmn6.
 ,Account_Funding_Source
 ,Payment_Method
 ,Purchase_Method
 ,Market_Segment
 ,Merchant_DBA
 ,Merchant_ZipCode
 ,ZIPCITY(Merchant_ZipCode) as Merchant_City
/* ,ZIPSTATE(Merchant_ZipCode) as Merchant_State*/
 ,Trans_Cnt
 ,Trans_Amt
,IntChgFee_Income 
from work.marketsummaryone 
;
Quit;
/*Addition of State Column from Zip Code */;
 proc sql;
 create table work.marketsummarythree as
 Select distinct
 Monthend_Date
 ,Month_n format=yymmn6.
 ,Account_Funding_Source
 ,Payment_Method
 ,Purchase_Method
 ,Market_Segment
 ,Merchant_DBA
 ,Merchant_ZipCode
 ,Merchant_City
 ,ZIPSTATE(Merchant_ZipCode) as Merchant_State
 ,Trans_Cnt
 ,Trans_Amt
,IntChgFee_Income 
from work.marketsummarytwo 
;
Quit;
/*Addition of County_VA column for understanding distribution of credit and debit metrics by county*/
proc sql;
create table work.marketsummary as
Select Distinct
t1.Monthend_Date
,t1.Month_n
,t1.Account_Funding_Source
,t1.Market_Segment
,t1.Merchant_DBA
,t1.Merchant_ZipCode
,(scan(t1.Merchant_City,1,',')) as Merchant_City
,t1.Merchant_State
,t1.Payment_Method
,t1.Purchase_Method as MOTO_ECIGroup
,t1.Trans_Cnt 
,t1.Trans_Amt
,t1.IntChgFee_Income as Issuer_Fee_Amt
	,t2.County_VA
from work.marketsummarythree t1
Left Join analytdm.DimGeography t2 on (t1.Merchant_ZipCode=t2.ZipCode)
where t1.Month_n NOT is missing;
Quit;
/* Saving in autoload folder for creating dashboard/visuals in SAS Visual Analytics 7.4 */;
data autoload.marketsummarydata_musa;
set work.marketsummary;
run;
/* # CREDIT CARDHOLDERS derivation from dbo*/
PROC SQL;
	CREATE TABLE WORK.PRIMARY_SSN AS
	SELECT DISTINCT t1.Monthenddate,
		t1.SSNPrimaryAccountHolder AS SSN
	FROM DBODIM.FACTSNAPSHOTFDR t1
	WHERE t1.StatusExternal NOT = 'Charged Off' AND t1.SSNPrimaryAccountHolder NOT IN ('0','','000000000','        .') 
AND t1.MonthendDate ge '31Oct2017'd;
QUIT;
PROC SQL;
	CREATE TABLE WORK.SECONDARY_SSN AS
	SELECT DISTINCT t1.Monthenddate,
		t1.SSNSecondaryAccountHolder AS SSN
	FROM DBODIM.FACTSNAPSHOTFDR t1
	WHERE t1.StatusExternal NOT = 'Charged Off' AND t1.SSNPrimaryAccountHolder NOT IN ('0','','000000000','        .') AND t1.MonthendDate ge '31Oct2017'd;
QUIT;
PROC SQL;
	CREATE TABLE WORK.CC_SSN AS
	SELECT * FROM WORK.PRIMARY_SSN
	UNION CORRESPONDING	SELECT * FROM WORK.SECONDARY_SSN;
QUIT;
/* # ACTIVE CREDIT CARDHOLDERS - CURRENT MONTH */
DATA WORK.CC_TRANS;
	SET AUTOLOAD.CARDSUMMARYDATA_MUSA (WHERE=(CREDIT_DEBIT = 'Credit'));
RUN;
PROC SQL;
	CREATE TABLE WORK.CC_TRANS_PRIMARY AS
	SELECT DISTINCT t1.Monthend_date,
		t1.Card_num,
		t2.SSNPrimaryAccountHolder AS SSN
	FROM AUTOLOAD.CARDSUMMARYDATA_MUSA t1
	LEFT JOIN DBODIM.FactSnapshotFDR t2 ON (t1.Monthend_date = t2.Monthenddate AND t1.Card_num = t2.AccountNumeric)
	WHERE t1.Credit_Debit = 'Credit' AND t2.SSNPrimaryAccountHolder NOT IN ('0','','000000000','        .') and t2.MonthendDate ge '31Oct2017'd;
QUIT;		
PROC SQL;
	CREATE TABLE WORK.CC_TRANS_SECONDARY AS
	SELECT DISTINCT t1.Monthend_date,
		t1.Card_num,
		t2.SSNSecondaryAccountHolder AS SSN
	FROM AUTOLOAD.CARDSUMMARYDATA_MUSA t1
	LEFT JOIN DBODIM.FactSnapshotFDR t2 ON (t1.Monthend_date = t2.Monthenddate AND t1.Card_num = t2.AccountNumeric)
	WHERE t1.Credit_Debit = 'Credit' AND t2.SSNSecondaryAccountHolder NOT IN ('0','','000000000','        .') and t2.MonthendDate ge '31Oct2017'd;
QUIT;
PROC SQL;
	CREATE TABLE WORK.CC_SSN_ACTIVE AS
	SELECT Monthend_date, SSN FROM WORK.CC_TRANS_PRIMARY
	UNION CORRESPONDING	SELECT Monthend_date, SSN FROM WORK.CC_TRANS_SECONDARY;
QUIT;
/*DISTINGUISHING MEMBERS, CREDIT CARD HOLDERS, AND ACTIVE CREDIT CARD HOLDERS*/;
PROC SQL;
	CREATE TABLE WORK.SSN_WITH_CC AS
	SELECT DISTINCT t1.Monthenddate,
		t1.SSN,
		CASE WHEN t1.Member_Definition = 1 THEN 1 ELSE 0 END AS Member,
		CASE WHEN t2.SSN IS MISSING THEN 0 ELSE 1 END AS CC_Cardholder,
		CASE WHEN t3.SSN IS MISSING THEN 0 ELSE 1 END AS ActiveCC_Cardholder
	FROM nestor.MEMBER_DEFINITION_AD2 t1
	LEFT JOIN WORK.CC_SSN t2 ON (t1.Monthenddate = t2.Monthenddate AND t1.SSN = t2.SSN)
	LEFT JOIN WORK.CC_SSN_ACTIVE t3 ON (t1.Monthenddate = t3.Monthend_date AND t1.SSN = t3.SSN) where t1.MonthendDate ge '31Oct2017'd;
QUIT;
PROC SQL;
	CREATE TABLE WORK.CCHOLDERMEMBERACTIVE AS
	SELECT distinct
		MonthendDate,
		(sum(Member)) as MemberCount format=COMMA23.
		,(sum(CC_Cardholder)) as CHCount format=COMMA23.
		,(sum(ActiveCC_Cardholder)) as ActiveCHCount format=COMMA23.
		from work.SSN_WITH_CC
		where MonthendDate le '31Mar2020'd
	group by MonthendDate ;
QUIT;
PROC SQL;
	CREATE TABLE WORK.CREDITMETRICS AS
	Select Distinct
	Monthend_Date
	,(Sum(Trans_Cnt)) as TransCount format=comma23.
	,(Sum(Trans_Amt)) as TransAmount format=dollar23.
	,(Sum(IntChgFee_Income)) as IntChgIncome format=dollar23.
	from autoload.Cardsummarydata_musa 
	where Credit_Debit='Credit'
	Group by Monthend_Date;
Quit;
PROC SQL;
CREATE TABLE WORK.CCMETRICSMBRS AS
	SELECT DISTINCT
	t1.MonthendDate
	,t1.MemberCount
	,t1.CHCount
	,t1.ActiveCHCount
		,t2.TransCount
		,t2.TransAmount
		,t2.IntChgIncome
	from work.CCHOLDERMEMBERACTIVE t1
		INNER JOIN work.CREDITMETRICS t2 on (t1.MonthendDate=t2.Monthend_Date)
		;
QUIT;
data autoload.CCMETRICSMBRS_MUSA;
	Set work.CCMETRICSMBRS;
run;
/* ESTABLISHING ACTIVE DEBIT CARD HOLDERS */;
PROC SQL;
   CREATE TABLE WORK.ATMCARDMASTER1 AS 
   SELECT /* Account2 */
            ((put(t1.Account,Z20.))) AS Account2, 
          t1.ACCOUNT, 
          t1.SHORT_NAME
      FROM ANALYTDM.ATM_CARD_MASTER t1;
QUIT;
PROC SQL;
   CREATE TABLE WORK.ATMCARDMASTER2 AS 
   SELECT t1.Account2, 
          t1.ACCOUNT, 
          t2.C_SSN, 
          t2.SSN, 
          t1.SHORT_NAME, 
          t2.C_TYPE
      FROM WORK.ATMCARDMASTER1 t1
           LEFT JOIN ANALYTDM.ssnCrossReference t2 ON (t1.Account2 = t2.C_ACCOUNT);
QUIT;
PROC SQL;
	CREATE TABLE WORK.SSN_WITH_DC AS
	SELECT DISTINCT t1.Monthenddate,
		t1.SSN,
		CASE WHEN t1.Member_Definition = 1 THEN 1 ELSE 0 END AS Member,
		CASE WHEN t3.SSN IS MISSING THEN 0 ELSE 1 END AS DC_ActiveCardholder
	FROM nestor.MEMBER_DEFINITION_AD2 t1
	LEFT JOIN WORK.ATMCARDMASTER2 t2 ON (t1.SSN = t2.SSN)
	LEFT JOIN autoload.cardsummarydata_musa t3 ON (t1.Monthenddate = t3.Monthend_date AND t1.SSN = t3.SSN) where t1.MonthendDate ge '31Oct2017'd;
QUIT;
PROC SQL;
	CREATE TABLE WORK.DCHOLDERMEMBERACTIVE AS
	SELECT distinct
		MonthendDate,
		(sum(Member)) as MemberCount format=COMMA23.
		,(sum(DC_ActiveCardholder)) as ActiveDHCount format=COMMA23.
		from work.SSN_WITH_DC
		where MonthendDate le '31Mar2020'd
	group by MonthendDate ;
QUIT;
/*Debit Card Metrics*/;
PROC SQL;
	CREATE TABLE WORK.DEBITMETRICS AS
	Select Distinct
	Monthend_Date
	,(Sum(Trans_Cnt)) as TransCount format=comma23.
	,(Sum(Trans_Amt)) as TransAmount format=dollar23.
	,(Sum(IntChgFee_Income)) as IntChgIncome format=dollar23.
	from autoload.Cardsummarydata_musa 
	where Credit_Debit='Debit'
	Group by Monthend_Date;
Quit;
PROC SQL;
CREATE TABLE WORK.DCMETRICSMBRS AS
	SELECT DISTINCT
	t1.MonthendDate
	,t1.MemberCount
	,t1.ActiveDHCount
		,t2.TransCount
		,t2.TransAmount
		,t2.IntChgIncome
	from work.DCHOLDERMEMBERACTIVE t1
		INNER JOIN work.DEBITMETRICS t2 on (t1.MonthendDate=t2.Monthend_Date)
		;
QUIT;
data autoload.DCMETRICSMBRS_MUSA;
	Set work.DCMETRICSMBRS;
run;
DATA WORK.MEMBER;
	SET ANALYTDM.MASTER_MEMBER (WHERE=(CURRENTRECORD = 'Y'));
RUN;
/*Interchange Income */;
PROC SQL;
	CREATE TABLE WORK.CARDSUMMARYDATA2 AS
	SELECT t1.Month_n, 
          t1.Monthend_Date, 
          t1.Quarter_n, 
          t1.Card_Num, 
          t1.CARD_NO, 
          t1.Credit_Debit, 
          t1.Network, 
          t1.Payment_Method, 
          t1.Purchase_Method, 
          t1.Trans_Cnt, 
          t1.Trans_Amt, 
          t1.IntChgFee_Income,
		  t2.SSN,
		  t3.MemberAddressCity, 
          t3.MemberAddressState, 
          t3.MemberAddressZipCode, 
          t3.MemberAddressLatitude, 
          t3.MemberAddressLongitude, 
          t3.BirthDate, 
          t3.Gender, 
          t3.MaritalStatus
      FROM WORK.CARDSUMMARYDATA1 t1
	  LEFT JOIN WORK.ATMCARDMASTER2 t2 ON (t1.CARD_NO = t2.ACCOUNT2)
	  LEFT JOIN WORK.MEMBER t3 ON (t2.SSN = t3.SSN);
QUIT;
PROC SQL;
	CREATE TABLE WORK.SSN_WITH_CdtDbtIntChg AS
	SELECT DISTINCT 
	t1.Monthenddate
	,t1.SSN
	,t1.Age
	,t1.Age_Range
	,t1.CreditScore
	,t1.Credit_Score_Range
			,t2.SSNPrimaryAccountHolder as Credit_Debit_User_SSN
			,t2.Card_Account_Num
			,t2.Issuer_Fee_Amt
			,t2.Account_Funding_Source,
		CASE WHEN t1.Member_Definition = 1 THEN 1 ELSE 0 END AS Member,
		CASE WHEN t2.SSNPrimaryAccountHolder IS MISSING THEN 0 ELSE 1 END AS Card_User
	FROM nestor.member_definition_ad2 t1
	LEFT JOIN cardanal.CARDSUMMARYDATA t2 ON (t1.Monthenddate = t2.Monthenddate AND t1.SSN = t2.SSNPrimaryAccountHolder);
QUIT;
PROC SQL;
Create Table work.CdtDbtIntChg_MUSA as
Select Distinct *
FROM work.SSN_WITH_CdtDbtIntChg;
Quit;
data autoload.CdtDbtIntChg_MUSA;
	set work.CdtDbtIntChg_MUSA;
run;
/* CREATING CREDIT CARD HOLDER MEMBER DELINQUENCIES */;
PROC SQL;
	CREATE TABLE WORK.CC_DELINQUENT_PRIMARY AS
	SELECT DISTINCT t1.Monthenddate,
		t1.SSNPrimaryAccountHolder AS SSN,
		t1.AmountDelinquent FORMAT=DOLLAR23.2 as AmountDelinquent
	FROM ANALYTDM.MASTER_LOAN_ROLLING24MO t1
;
QUIT;
PROC SQL;
	CREATE TABLE WORK.CC_Delinquent_Secondary AS
	SELECT DISTINCT t1.Monthenddate,
	
		t1.SSNSecondaryAccountHolder AS SSN,
		t1.Amountdelinquent FORMAT=DOLLAR23.2 as AmountDelinquent
	
	FROM ANALYTDM.MASTER_LOAN_ROLLING24MO  t1
	;
QUIT;
PROC SQL;
CREATE TABLE WORK.CC_Delinquent AS
	SELECT DISTINCT Monthenddate, SSN, AmountDelinquent FROM WORK.CC_DELINQUENT_PRIMARY
	UNION CORRESPONDING	SELECT Monthenddate, SSN, AmountDelinquent FROM WORK.CC_DELINQUENT_PRIMARY;
QUIT;
proc sql;
create table work.Delinquent_Members as
Select DISTINCT t1.MonthendDate,
t1.SSN,
t1.Age,
t1.Age_Range,
t1.CreditScore,
t1.Credit_Score_Range,
t2.SSN as Delinquent_CC_Member,
t2.AmountDelinquent,
		CASE WHEN t1.Member_Definition = 1 THEN 1 ELSE 0 END AS Member,
		CASE WHEN t2.AmountDelinquent > 0 THEN 1 ELSE 0 END AS Delinquent_CC_Cardholder

FROM nestor.member_definition_ad2 t1
	LEFT JOIN WORK.CC_Delinquent t2 ON (t1.Monthenddate = t2.Monthenddate AND t1.SSN = t2.SSN);
QUIT;
/*Delinquent Credit Card Holders Metrics*/;
PROC SQL;
	CREATE TABLE WORK.CCDBO_DELINQUENT_PRIMARY AS
	SELECT DISTINCT t1.Monthenddate,
		t1.SSNPrimaryAccountHolder AS SSN,
		t1.AmountDelinquent FORMAT=DOLLAR23.2 as AmountDelinquent,
			t2.Age,
			t2.Age_Range,
			t2.CreditScore,
			t2.Credit_Score_Range
  	FROM dbodim.FactsnapshotFDR t1
	LEFT JOIN nestor.member_definition_ad2 t2 on (t1.SSNPrimaryAccountHolder=t2.SSN)
;
QUIT;		
PROC SQL;
	CREATE TABLE WORK.CCDBO_Delinquent_Secondary AS
	SELECT DISTINCT t1.Monthenddate,
		t1.SSNSecondaryAccountHolder AS SSN,
		t1.Amountdelinquent FORMAT=DOLLAR23.2 as AmountDelinquent,
			t2.Age,
			t2.Age_Range,
			t2.CreditScore,
			t2.Credit_Score_Range
	FROM dbodim.FactSnapshotFDR t1	
	LEFT JOIN nestor.member_definition_ad2 t2 on (t1.SSNPrimaryAccountHolder=t2.SSN)
;
QUIT;
PROC SQL;
CREATE TABLE WORK.CCDBO_Delinquent AS
	SELECT DISTINCT * FROM WORK.CCDBO_DELINQUENT_PRIMARY
	UNION	SELECT * FROM WORK.CCDBO_DELINQUENT_PRIMARY;
QUIT;
proc sql;
create table work.CCDBO_Delinquent_Members as
Select DISTINCT t1.MonthendDate,
t1.SSN,
t1.Age,
t1.Age_Range,
t1.CreditScore,
t1.Credit_Score_Range,
t1.AmountDelinquent
FROM WORK.CCDBO_Delinquent t1 
WHERE t1.AmountDelinquent>0;
QUIT;

PROC SQL;
   CREATE TABLE WORK.DELINQUENT_Member AS 
   SELECT DISTINCT t1.MonthEndDate, 
          /* Delinquent_Member_Count */
            (Count(t1.SSN)) FORMAT=COMMA10. AS Delinq_MemberCount, 
          /* Avg_Delinq_Bal */
            ((SUM(t1.AmountDelinquent))/(COUNT(t1.SSN))) FORMAT=DOLLAR10. AS Avg_Delinq_Bal
			
      FROM WORK.CCDBO_DELINQUENT_MEMBERS t1
      GROUP BY t1.MonthEndDate;
QUIT;
data autoload.CDT_DelinqtMembers_MuSa;
	Set work.delinquent_member;
run;
PROC SQL;
   CREATE TABLE work.CdtDelinquentPercent_MuSa AS 
   SELECT DISTINCT t1.MonthEndDate, 
          /* Total_AmountDelinquent */
            (SUM(t1.AmountDelinquent)) FORMAT=DOLLAR23. AS Total_AmountDelinquent, 
          /* Total_CurrentBalance */
            (SUM(t1.CurrentBalance)) FORMAT=DOLLAR23. AS Total_CurrentBalance, 
          /* Delinquent_Percent */
            ((SUM(t1.AmountDelinquent))/(SUM(t1.CurrentBalance))) FORMAT=PERCENT6.2 AS Delinquent_Percent
      FROM DBODIM.FactSnapshotFDR t1
      WHERE t1.StatusExternal NOT IN 
           (
           'Charged Off'
           )
      GROUP BY t1.MonthEndDate;
QUIT;

PROC SQL;
   CREATE TABLE WORK.Delinq_Total_Balance AS 
   SELECT DISTINCT t1.MonthEndDate, 
          t1.Delinq_CCHolders, 
          t1.Monthly_DelinquentBalance, 
          t2.CreditCard_Holders, 
          t2.SUM_of_CurrentBalance FORMAT=DOLLAR23. AS Total_CurrentBalance
      FROM WORK.DELINQUENT_CREDITDATA t1
           INNER JOIN WORK.CURRENT_BALANCE t2 ON (t1.MonthEndDate = t2.MonthEndDate);
QUIT;
/*CREDIT TRANSACTIONS DATA AND METRICS */;
proc sql;
 create table work.CREDITTRANSDATA as
 Select distinct

 Date
,MonthendDate
 ,AccountNumeric as Card_Account_Num
 ,SSNPrimaryAccountHolder
 ,SSNSecondaryAccountHolder
 ,CreditDebit as Account_Funding_Source
 ,Payment_Method 
 ,Purchase_Method 
 ,Market_Segment
 ,Merchant_DBA
 ,Merchant_ZipCode
 ,(sum(Trans_Cnt)) as TransCnt format=comma23.
 ,(sum(Trans_Amt)) as TransAmt format=dollar23.2
,(sum(Issuer_Fee_Amt)) as IssuerFeeAmt format=dollar23.2
from ANALGP.VVO_CARDTRANSACTIONS 
where MonthendDate le '31Mar2020'd and CreditDebit='Credit'
Group by MonthendDate, AccountNumeric, SSNPrimaryAccountHOlder, SSNSecondaryAccountHolder, CreditDebit, Payment_Method, Purchase_Method, Market_Segment, Merchant_DBA, Merchant_ZipCode
order by MonthendDate, AccountNumeric, SSNPrimaryAccountHolder, SSNSecondaryAccountHolder, CreditDebit, Payment_Method, Purchase_Method, Market_Segment, Merchant_DBA, Merchant_ZipCode
;
Quit;
PROC SQL;
	CREATE TABLE WORK.CREDITTRANS_1 AS
	SELECT DISTINCT 
	t1.Date
		,t1.MonthendDate 
		,t1.Card_Account_Num
        ,t1.Account_Funding_Source
        ,t1.Payment_Method
		,t1.Purchase_Method
        ,t1.TransCnt 
        ,t1.TransAmt 
        ,t1.IssuerFeeAmt as IntChg_Fee
		,t1.Market_Segment
		,t1.Merchant_DBA as Merchant
		,t1.Merchant_ZipCode
			,t2.SSNPrimaryAccountHolder AS SSN
			,t2.LatestBeaconScore
		  		,t3.MemberAddressCity 
          		,t3.MemberAddressState 
          		,t3.MemberAddressZipCode 
          		,t3.MemberAddressLatitude 
          		,t3.MemberAddressLongitude 
          		,t3.BirthDate
          		,t3.Gender 
          		,t3.MaritalStatus
      FROM work.CREDITTRANSDATA t1
	  LEFT JOIN ANALYTDM.MASTER_LOAN_ROLLING24MO t2 ON (t1.MonthendDate = t2.MonthEndDate AND t1.Card_Account_Num = t2.AccountNumeric)
	  LEFT JOIN WORK.MEMBER t3 ON (t2.SSNPrimaryAccountHolder = t3.SSN)
		;
QUIT;

PROC SQL;
	CREATE TABLE WORK.CREDITTRANSDATA AS
	SELECT date 
        ,intnx('Month', date, 0, 'same') as Month_n format=yymmn6.
		,MonthendDate 
        ,intnx('Quarter', Date, 0, 'same') as Quarter_n format=yyQ6.
        ,Card_Account_Num
        ,Account_Funding_Source
        ,Payment_Method
		,Purchase_Method 
        ,TransCnt 
        ,TransAmt 
        ,IntChg_Fee
		,Market_Segment
		,Merchant
		,Merchant_ZipCode
		,SSN
		,LatestBeaconScore
		,MemberAddressCity 
        ,MemberAddressState 
        ,MemberAddressZipCode 
        ,MemberAddressLatitude 
        ,MemberAddressLongitude 
        ,BirthDate
        ,Gender 
        ,MaritalStatus,
	(CASE
		
		WHEN LatestBeaconScore BETWEEN 0 AND 579 THEN '<=579'
		WHEN LatestBeaconScore BETWEEN 580 AND 669 THEN '580-669'
		WHEN LatestBeaconScore BETWEEN 670 AND 739 THEN '670-739'
		WHEN LatestBeaconScore BETWEEN 740 AND 799 THEN '740-799'
		WHEN LatestBeaconScore GE 800 THEN '>=800'
		ELSE 'Missing'
		END) AS CredScr_Range	
			,yrdif(BirthDate,MonthendDate,'ACT/365')  as  AGE,  
(	CASE
		
		WHEN CALCULATED AGE BETWEEN 0 AND 18 THEN '0-18'
		WHEN CALCULATED AGE GT 18 AND CALCULATED AGE LE 25 THEN '19-25'
		WHEN CALCULATED AGE GT 25 AND CALCULATED AGE LE 35 THEN '26-35'
		WHEN CALCULATED AGE GT 35 AND CALCULATED AGE LE 45 THEN '36-45'
		WHEN CALCULATED AGE GT 45 AND CALCULATED AGE LE 55 THEN '46-55'
		WHEN CALCULATED AGE GT 55 AND CALCULATED AGE LE 65 THEN '56-65'
		WHEN CALCULATED AGE GT 65 AND CALCULATED AGE LE 75 THEN '66-75'
		WHEN CALCULATED AGE GT 75 THEN '75+'
		ELSE 'Missing'
		END) AS AGE_RANGE 
	FROM WORK.CREDITTRANS_1;	
QUIT;
data lpeel.CREDITTRANSDATA1_LP;
		set WORK.CREDITTRANSDATA;
	format Age 4.1 Trans_Cnt 8. Trans_Amt dollar12. IntChg_Fee dollar8. ;
	run;