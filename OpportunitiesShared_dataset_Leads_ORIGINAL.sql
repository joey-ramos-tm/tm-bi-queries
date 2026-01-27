/****** Script for SelectTopNRows command from SSMS  ******/
---Report: Opportunities Shared_dataset
---Data Source: Leads
---Author: Joey Ramos

DROP TABLE IF EXISTS [#Sale];

SELECT 
DISTINCT
	[SaleDataSource] = SD.DataSource 
	,[Quote] = SD.QuoteReferenceName 
	,[SaleDateApproved] = SD.ApprovalDate 
	,[SaleEmailAddress] = SD.EmailAddress 
	,[SaleCoBuyerEmailAddress] = SD.CoBuyerEmailAddress 
	,[SaleRealtorName] = SD.RealtorNamer 
	,[SaleCommunityID] = SD.CommunityId 
	,[SaleCommunityName] = COALESCE(CM.MarketingName,SD.CommunityName)
	,[SaleDivisionName] = 
		CASE
	   		WHEN SD.NSEDivisionName IN ( 'Treasure Coast', 'Sarasota' ) THEN 'Sarasota'
	   		ELSE SD.NSEDivisionName
	  	END
	,[NSECommunityName] = SD.NSECompanyName
	,[compcode] = SD.CompanyCode
	,[NSEDivisionName] = SD.NSEDivisionName
	,[SaleAgentFullName] = SD.AgentFullName
	,[SaleEarnestDepositAmount] = SD.EarnestDepositAmount
	,[NetSalesPrice] = SD.NetSalesPriceAmount
	,[LotID] = SD.Lot
	,[SaleBuyerName] = SD.BuyerName
	,[SaleIsIHCSale] = SD.IHCIndicator
	,[OpportunityID] = SD.OpportunityID
	,[AccountID] = SD.AccountId
INTO [#Sale]
FROM TaylorMorrisonDWH_Gold.Sales.SaleDetail SD 
	INNER JOIN TaylorMorrisonDWH_Gold.[ReferenceData].[Community] CM
		ON SD.CompanyCode = CM.CommunityCode
			AND SD.OperatingUnit = CM.OperatingUnit
			AND SD.ProjectId = CM.ProjectId
WHERE 
	SD.ExcludeShellLotIndicator = 0
		AND SD.ExcludeCancelIndicator = 0
		AND ( ( SD.EmailAddress IS NOT NULL AND SD.EmailAddress  <> '' )
			OR ( SD.CoBuyerEmailAddress  IS NOT NULL AND SD.CoBuyerEmailAddress  <> '' ) )
;

/*

SELECT * FROM [#Sale] 
WHERE OpportunityID IN (
'006VY00000Haf1FYAR'
)
*/

DROP TABLE IF EXISTS [#LeadsStart];

SELECT
[OpportunityID]	= OPP.OpportunityID
,[Account Row Number] = ROW_NUMBER() OVER ( PARTITION BY OPP.AccountId ORDER BY OPP.OpportunityCreateTimestamp)
---,[Opportunity Row Number] = ROW_NUMBER() OVER ( PARTITION BY OPP.AccountId, DATEADD(mm, DATEDIFF(mm, 0, OPP.OpportunityCreateTimestamp), 0) ORDER BY OPP.OpportunityCreateTimestamp ASC)
,[Opportunity Row Number] =	ROW_NUMBER() OVER ( PARTITION BY [Opp].AccountFullName
								  , DATEADD(mm, DATEDIFF(mm, 0, CAST( [Opp].[OpportunityCreateTimestamp] AS DATE)), 0)
								  ORDER BY  [Opp].[OpportunityCreateTimestamp]  ASC
								) 
,[Opportunity Row Number Desc] = ROW_NUMBER() OVER ( PARTITION BY OPP.AccountId ORDER BY OPP.OpportunityCreateTimestamp DESC)
,[Lead Source] = 
	CASE
		WHEN OPP.[Lead Source] IN ( 'Booking', 'Scheduler - External' ) THEN 'External Appointment'
		WHEN OPP.[Lead Source] IN ( 'Scheduler - Internal' ) THEN 'Internal Appointment'
		WHEN OPP.[Lead Source] IN ( 'BuilderBOT' ) THEN 'Chat'
		WHEN OPP.[Lead Source] IN ( 'uTour - Cancelled' ) THEN 'Self-Guided Tour- Cancelled'
		WHEN OPP.[Lead Source] IN ( 'uTour - Completed' ) THEN 'Self-Guided Tour- Completed'
		WHEN OPP.[Lead Source] IN ( 'uTour - Loaded' ) THEN 'Self-Guided Tour- Loaded'
		WHEN OPP.[Lead Source] IN ( 'uTour - Deleted' ) THEN 'Self-Guided Tour- Deleted'
		WHEN OPP.[Lead Source] IN ( 'uTour - Missed' ) THEN 'Self-Guided Tour- Missed'
		WHEN OPP.[Lead Source] IN ( 'uTour - Rescheduled' ) THEN 'Self-Guided Tour- Rescheduled'
		WHEN OPP.[Lead Source] IN ( 'Reservation Portal Inventory', '3Dcart', '3Dcart - Loaded','Reservation Portal Inventory Primary' , 'Reservation Portal Inventory Backup' ) THEN 'Home Reservations - QMI'
		WHEN OPP.[Lead Source] IN ( 'Reservation Portal TBB' ,'Reservation Portal TBB Primary' ,'Reservation Portal TBB backup') THEN 'Home Reservations - TBB'
		ELSE OPP.[Lead Source]
	END 	
	 , [Division]	 = 
			CASE												
				WHEN [Opp].SalePrimaryDivisionName  IS NOT NULL THEN [Opp].SalePrimaryDivisionName 
				WHEN [Opp].SalePrimaryDivisionName  IS NULL THEN [SitecoreCommunity].[DivisionName] 
				WHEN ( [Opp].SiteCoreCommunityTMDivisionName IS NULL ) THEN SiteCoreCommunity.[DivisionName]
				ELSE [Opp].SiteCoreCommunityTMDivisionName
				END 		
,[CommunityID] = OPP.SitecoreCommunityId
,[Community] = OPP.SaleCommunityName
,Opp.OpportunityCreateTimestamp
,[Email] = OPP.AccountPersonEmail
,[Owner Name] = OPP.[Owner Name]
,[Owner Id] = OPP.[OpportunityOwnerId]
,[Stage] = OPP.OpportunityStageName
,[Connection Name]	= OPP.OpportunityName
,[OSM Rating] = OPP.IHCRating
,[OSM Lead Source]	= OPP.IHCLeadSource
,[Contact Name] = OPP.AccountFullName
,[Contact Phone] = OPP.AccountPersonMobilePhone	
,[Agent Email]	= OPP.AccountPersonEmail											
,[Realtor vs Customer] = 
	CASE 
		WHEN OPP.AccountPersonEmail IS NOT NULL THEN 'Customer'
		WHEN OPP.AccountPersonEmail IS NULL AND OPP.AccountPersonEmail IS NOT NULL THEN 'Realtor' 
		ELSE NULL 
	END
,[Coming Soon Communities]	= OPP.OpportunityComingSoonIndicator --comming soon communities
,[Active Communities] = OPP.ActiveCommunityIndicator -- active communities
,[Community Status] = OPP.CommunityStatusCode
----------------------------------New Stuff 
,Lead_Ranking__c = OPP.OpportunityLeadRanking
,Lead_Ranking_Modified_Date__c = OPP.OpportunityLeadRankingModifyDate
,Lead_Reference_ID__c = OPP.OpportunityLeadReferenceId
,Prospect_Rating__c = OPP.OpportunityProspectRating
,Agent_License_number__c = OPP.OpportunityAgentLicense
,How_did_you_hear_about_us__c = OPP.OpportunityHowDidYouHearAboutUs
,Move_in_range__c = OPP.OpportunityMoveInRange
,Price_Range__c = OPP.OpportunityPriceRange
,Reg_Card_Created_date__c = OPP.OpportunityRegistrationCreateTimestamp
,Reservation_No_Agent__C = OPP.OpportunityReservationNoAgentIndicator
,I_Plan_to_purchase_a_home__C = OPP.OpportunityPlanToPurchaseHomeIndicator
,PersonMailingPostalCode = OPP.AccountPersonMailingPostalCode
,PersonMailingStreet = OPP.AccountPersonMailingMailingStreet
,PersonMailingCity = OPP.AccountPersonMailingCity
,PersonMailingState = OPP.AccountPersonMailingState
,PersonMailingStateCode = OPP.AccountPersonMailingStateCode
,[Other Builder] = OPP.OpportunityOtherBuilder -- Lostop report:  Why did they purchase elsewhere/additional comments open text field.
,[Why] = OPP.OpportunityWhy-- Lostop report: 
,IsRealtor__pc  = OPP.AccountIsRealtorIndicator
,Lost_Opportunity__c = OPP.LostOpportunityIndicator
,What_Did_They_Purchase__c = OPP.OpportunityWhatDidTheyPurchase
,DataLastRefreshed = 'Data Last Refreshed: ' + CAST( FORMAT(GETDATE(),'MM/dd/yyyy hh:mm:ss tt') AS VARCHAR(30)) + ' AZ/MST'
,[Created By] = 
	CASE 
		WHEN OPP.UserProfileId = '00e41000000OzOQAA0' THEN 'ASM' 
		WHEN OPP.UserProfileId = '00e41000000OzXMAA0' THEN 'CSM'
		ELSE 'OSM' 
	END
,ProfileId = OPP.UserProfileId
,[AccountID] = OPP.AccountId
,[ContactID] = Contact.CONTACT_ID
INTO [#LeadsStart]
FROM TaylorMorrisonDWH_Gold.Sales.SaleOpportunityDetail OPP  
	LEFT JOIN [TaylorMorrisonDWH_Silver].[SILVER_DB].[CONTACT] Contact
		ON OPP.AccountId = Contact.ACCT_ID
			AND Contact.ROW_CURR_IND = 1
	LEFT JOIN TaylorMorrisonDWH_Gold.[ReferenceData].[SiteCoreCommunity] SiteCoreCommunity
		ON [Opp].SitecoreCommunityId = SiteCoreCommunity.CommunityId
       
WHERE ( OPP.[ContactEmail] IS NULL
		OR ( OPP.[ContactEmail] NOT LIKE '%Taylormorrison.com'
		AND OPP.[ContactEmail] NOT LIKE '%12starsmedia.com'
		AND OPP.[ContactEmail] NOT LIKE '%oakcreektrail.com'
		AND OPP.[ContactEmail] NOT LIKE '%@mailinator.com%') )
		AND CAST(OPP.OpportunityCreateTimestamp AS DATE) BETWEEN DATEADD(dd, 1, EOMONTH(DATEADD(mm, -26, GETDATE()))) AND GETDATE() --last 24 months including this month
		AND OPP.UserProfileId <> '00e41000000OzOQAA0' AND OPP.UserProfileId <> '00e41000000OzXMAA0' AND OPP.UserProfileId <> '00e41000000OzORAA0' --Exclude ASM/CSM/CSA
				AND OPP.AccountFullName NOT LIKE '%test%'
;

/*
SELECT *
FROM  [#LeadsStart]
WHERE AccountId = '001VY00000HFumYYAT'
---WHERE OpportunityID = '006VY00000Haf1FYAR'
*/

DROP TABLE IF EXISTS [#Leads];

SELECT
DISTINCT
OpportunityID
,[Account Row Number] 
,[Opportunity Row Number]
,[Opportunity Row Number Desc]
,[Lead Source]
,Division
,CommunityID
,Community
, [Created Month Opportunity]	= CAST(DATEADD(mm, DATEDIFF(mm, 0, CAST(DATEADD(hour, -7, OpportunityCreateTimestamp) AS DATE)), 0)  AS DATE)
, [Created Date Opportunity]	= CAST(DATEADD(hour, -7, OpportunityCreateTimestamp) AS DATE)
,Email
,[Owner Name]
,[Owner Id]
,Stage
,[Connection Name]
,[OSM Rating]
,[OSM Lead Source]
,[Contact Name]
,[Contact Phone]
,[Agent Email]
,[Realtor vs Customer]
,[Coming Soon Communities]
,[Active Communities]
,[Community Status]
,Lead_Ranking__c
,Lead_Ranking_Modified_Date__c
,Lead_Reference_ID__c
,Prospect_Rating__c
,Agent_License_number__c
,How_did_you_hear_about_us__c
,Move_in_range__c
,Price_Range__c
,Reg_Card_Created_date__c
,Reservation_No_Agent__C
,I_Plan_to_purchase_a_home__C
,PersonMailingPostalCode
,PersonMailingStreet
,PersonMailingCity
,PersonMailingState
,PersonMailingStateCode
,[Other Builder]
,Why
,IsRealtor__pc
,Lost_Opportunity__c
,What_Did_They_Purchase__c
,DataLastRefreshed
,[Created By]
,ProfileId
,AccountID
,ContactID
INTO [#Leads]
FROM [#LeadsStart]

/*
SELECT * FROM [#Sale] 
WHERE OpportunityID IN (
'0063f000007PEr6AAG',
'006VY00000A4nMBYAZ'
)

/*
SELECT *
FROM  [#Leads]
WHERE AccountId = '001VY00000HFumYYAT'
---WHERE OpportunityID = '006VY00000Haf1FYAR'
*/
*/

-----------------------------------------------------------------------------------------------------------------
--- LEADS DATA - COMPLETED
-----------------------------------------------------------------------------------------------------------------


SELECT
DISTINCT
  [Leads].[OpportunityID] 		
, [Leads].[Opportunity Row Number] 
,[Leads].[Account Row Number] 
, [Leads].[Lead Source]									
, [Leads].[Division] 								
, [Leads].[CommunityID]  											
, [Leads].[Community] 									
, [Leads].[Created Month Opportunity] 	
, [Leads].[Created Date Opportunity] 	
, [Leads].[Email] 
, [Leads].[Owner Name] 						
, [Leads].[Owner Id]
, [Leads].[Stage] 						
, [Leads].[Connection Name]			
, [Leads].[OSM Rating] 			
, [Leads].[OSM Lead Source]			
, [Leads].[Contact Name] 			
, [Leads].[Contact Phone] 			
, [Leads].[Agent Email]			
, [Leads].[Coming Soon Communities]	
, [Leads].[Active Communities] 	
, [Leads].[Community Status] 			
--------------------------------New Stuff 
, [Leads].Lead_Ranking__c
, [Leads].Lead_Ranking_Modified_Date__c 
, [Leads].Lead_Reference_ID__c 
, [Leads].Prospect_Rating__c 
, [Leads].Agent_License_number__c 
, [Leads].How_did_you_hear_about_us__c 
, [Leads].Move_in_range__c 
, [Leads].Price_Range__c 
, [Leads].Reg_Card_Created_date__c 
, [Leads].Reservation_No_Agent__C 
, [Leads].I_Plan_to_purchase_a_home__C 
, 'Completed' AS [Metric]
, NULL AS [SaleDateApproved]
, 1 AS [Quantity]
, NULL AS [Quote]
, NULL AS [SaleAgentFullName]
, NULL AS [SaleEarnestDepositAmount]
, NULL AS [NetSalesPrice]
, NULL AS [LotID]
, NULL AS [SaleRealtorName]
, NULL AS [SaleBuyerName]
, NULL AS [IHC Sale]
, [Leads].PersonMailingPostalCode
, [Leads].PersonMailingStreet
, [Leads].PersonMailingCity
, [Leads].PersonMailingState
, [Leads].PersonMailingStateCode
, [Leads].DataLastRefreshed
, [Leads].[Realtor vs Customer] 
, [Leads].[Other Builder] 
, [Leads].[Why]
, [Leads].IsRealtor__pc 
, [Leads].Lost_Opportunity__c 
, [Leads].What_Did_They_Purchase__c 
, [Source] = 'Leads'
, [Created By]
, [ProfileId]
,AccountID
,ContactID
-- select COUNT([Appointment].[Activity ID] ) -- before: 94,296 ; After adding Lead table before appt: 16,526
-- SELECT COUNT([Leads].[LeadSourceID] ) --before: 510,148; after adding lead table after appt:16,526
FROM [#Leads] [Leads] 

UNION ALL
-----------------------------------------------------------------------------------------------------------------------------------------
-- LEADS DATA - UNIQUE INDIVIDUALS
-----------------------------------------------------------------------------------------------------------------------------------------

SELECT
DISTINCT
 [Leads].[OpportunityID] 		
, [Leads].[Opportunity Row Number] 
,[Leads].[Account Row Number] 
, [Leads].[Lead Source]									
, [Leads].[Division] 								
, [Leads].[CommunityID]  											
, [Leads].[Community] 									
, [Leads].[Created Month Opportunity] 	
, [Leads].[Created Date Opportunity] 	
, [Leads].[Email] 
, [Leads].[Owner Name] 			
, [Leads].[Owner Id]
, [Leads].[Stage] 						
, [Leads].[Connection Name]			
, [Leads].[OSM Rating] 			
, [Leads].[OSM Lead Source]			
, [Leads].[Contact Name] 			
, [Leads].[Contact Phone] 			
, [Leads].[Agent Email]			
, [Leads].[Coming Soon Communities]	
, [Leads].[Active Communities] 	
, [Leads].[Community Status] 			
--------------------------------New Stuff 
, [Leads].Lead_Ranking__c
, [Leads].Lead_Ranking_Modified_Date__c 
, [Leads].Lead_Reference_ID__c 
, [Leads].Prospect_Rating__c 
, [Leads].Agent_License_number__c 
, [Leads].How_did_you_hear_about_us__c 
, [Leads].Move_in_range__c 
, [Leads].Price_Range__c 
, [Leads].Reg_Card_Created_date__c 
, [Leads].Reservation_No_Agent__C 
, [Leads].I_Plan_to_purchase_a_home__C 
, 'Unique Individuals' AS [Metric]
, NULL AS [SaleDateApproved]
, 1 AS [Quantity]
, NULL AS [Quote]
, NULL AS [SaleAgentFullName]
, NULL AS [SaleEarnestDepositAmount]
, NULL AS [NetSalesPrice]
, NULL AS [LotID]
, NULL AS [SaleRealtorName]
, NULL AS [SaleBuyerName]
, NULL AS [IHC Sale]
, [Leads].PersonMailingPostalCode
, [Leads].PersonMailingStreet
, [Leads].PersonMailingCity
, [Leads].PersonMailingState
, [Leads].PersonMailingStateCode
, [Leads].DataLastRefreshed
, [Leads].[Realtor vs Customer] 
, [Leads].[Other Builder] 
, [Leads].[Why]
, [Leads].IsRealtor__pc 
, [Leads].Lost_Opportunity__c 
, [Leads].What_Did_They_Purchase__c 
, [Source] = 'Leads'
,[Leads]. [Created By]
,[Leads].[ProfileId]
,[Leads].AccountID
,[Leads].ContactID
FROM [#Leads] [Leads]
WHERE [Leads].[Opportunity Row Number] = 1

-----------------------------------------------------------------------------------------------------------------------------------------
-- LEADS DATA - Purchased
-----------------------------------------------------------------------------------------------------------------------------------------
UNION ALL

SELECT  
DISTINCT
	  [x_Leads].[OpportunityID] 		
	, [x_Leads].[Opportunity Row Number] 
	,[x_Leads].[Account Row Number] 
	, [x_Leads].[Lead Source]									
	, [Sale].[SaleDivisionName] AS [Division]							
	, [x_Leads].[CommunityID]  											
	, [Sale].[SaleCommunityName] AS [Community]								
	, [x_Leads].[Created Month Opportunity] 	
	, [x_Leads].[Created Date Opportunity] 	
	, [x_Leads].[Email] 
	, [x_Leads].[Owner Name] 								
	, [x_Leads].[Owner Id]
	, [x_Leads].[Stage] 						
	, [x_Leads].[Connection Name]			
	, [x_Leads].[OSM Rating] 			
	, [x_Leads].[OSM Lead Source]			
	, [x_Leads].[Contact Name] 			
	, [x_Leads].[Contact Phone] 			
	, [x_Leads].[Agent Email]			
	, [x_Leads].[Coming Soon Communities]	
	, [x_Leads].[Active Communities] 	
	, [x_Leads].[Community Status] 			
	--------------------------------New Stuff 
	, [x_Leads].Lead_Ranking__c
	, [x_Leads].Lead_Ranking_Modified_Date__c 
	, [x_Leads].Lead_Reference_ID__c 
	, [x_Leads].Prospect_Rating__c 
	, [x_Leads].Agent_License_number__c 
	, [x_Leads].How_did_you_hear_about_us__c 
	, [x_Leads].Move_in_range__c 
	, [x_Leads].Price_Range__c 
	, [x_Leads].Reg_Card_Created_date__c 
	, [x_Leads].Reservation_No_Agent__C 
	, [x_Leads].I_Plan_to_purchase_a_home__C 
	, 'Purchased' AS [Metric]
	, [Sale].[SaleDateApproved]
	, 1 AS [Quantity]
	, [Sale].[Quote]
	, [Sale].[SaleAgentFullName]
	, [Sale].[SaleEarnestDepositAmount]
	, [Sale].[NetSalesPrice]
	, [Sale].[LotID]
	, [Sale].[SaleRealtorName]
	, [Sale].[SaleBuyerName]
	, [Sale].[SaleIsIHCSale] AS [IHC Sale]
	, [x_Leads].PersonMailingPostalCode
	, [x_Leads].PersonMailingStreet
	, [x_Leads].PersonMailingCity
	, [x_Leads].PersonMailingState
	, [x_Leads].PersonMailingStateCode
	, [x_Leads].DataLastRefreshed
	, [x_Leads].[Realtor vs Customer] 
	, [x_Leads].[Other Builder] 
	, [x_Leads].[Why]
	, [x_Leads].IsRealtor__pc 
	, [x_Leads].Lost_Opportunity__c 
	, [x_Leads].What_Did_They_Purchase__c 
	, [Source] = 'Leads'
	, [x_Leads].[Created By]
	, [x_Leads].[ProfileId]
	,[x_Leads].AccountID
	,[x_Leads].ContactID
FROM [#Sale] [Sale]
	LEFT JOIN [#Leads] [x_Leads]
		ON [x_Leads].[OpportunityID] = [Sale].[OpportunityID]
			AND CAST([Sale].[SaleDateApproved] AS DATE) >= CAST([x_Leads].[Created Date Opportunity]  AS DATE)
			AND [x_Leads].[Opportunity Row Number Desc] = 1

-----------------------------------------------------------------------------------------------------------------------------------------
-- LEADS DATA - - AVERAGE DAYS FROM LEAD TO PURCHASE
-----------------------------------------------------------------------------------------------------------------------------------------
UNION ALL

SELECT  
DISTINCT
	  [x_Leads].[OpportunityID] 		
	, [x_Leads].[Opportunity Row Number] 
	,[x_Leads].[Account Row Number] 
	, [x_Leads].[Lead Source]									
	, [Sale].[SaleDivisionName] AS [Division]							
	, [x_Leads].[CommunityID]  											
	, [Sale].[SaleCommunityName] AS [Community]								
	, [x_Leads].[Created Month Opportunity] 	
	, [x_Leads].[Created Date Opportunity] 	
	, [x_Leads].[Email] 
	, [x_Leads].[Owner Name] 								
	, [x_Leads].[Owner Id]
	, [x_Leads].[Stage] 						
	, [x_Leads].[Connection Name]			
	, [x_Leads].[OSM Rating] 			
	, [x_Leads].[OSM Lead Source]			
	, [x_Leads].[Contact Name] 			
	, [x_Leads].[Contact Phone] 			
	, [x_Leads].[Agent Email]			
	, [x_Leads].[Coming Soon Communities]	
	, [x_Leads].[Active Communities] 	
	, [x_Leads].[Community Status] 			
	--------------------------------New Stuff 
	, [x_Leads].Lead_Ranking__c
	, [x_Leads].Lead_Ranking_Modified_Date__c 
	, [x_Leads].Lead_Reference_ID__c 
	, [x_Leads].Prospect_Rating__c 
	, [x_Leads].Agent_License_number__c 
	, [x_Leads].How_did_you_hear_about_us__c 
	, [x_Leads].Move_in_range__c 
	, [x_Leads].Price_Range__c 
	, [x_Leads].Reg_Card_Created_date__c 
	, [x_Leads].Reservation_No_Agent__C 
	, [x_Leads].I_Plan_to_purchase_a_home__C 
	, 'Avg Days From Appointment to Purchase' AS [Metric]
	, [Sale].[SaleDateApproved]
	, DATEDIFF(DAY, CAST([x_Leads].[Created Date Opportunity] AS DATE), CAST([Sale].[SaleDateApproved] AS DATE)) AS [Quantity]
	, NULL AS [Quote]
	, NULL AS [SaleAgentFullName]
	, NULL AS [SaleEarnestDepositAmount]
	, NULL AS [NetSalesPrice]
	, NULL AS [LotID]
	, NULL AS [SaleRealtorName]
	, NULL AS [SaleBuyerName]
	, NULL AS [IHC Sale]
	, [x_Leads].PersonMailingPostalCode
	, [x_Leads].PersonMailingStreet
	, [x_Leads].PersonMailingCity
	, [x_Leads].PersonMailingState
	, [x_Leads].PersonMailingStateCode
	, [x_Leads].DataLastRefreshed
	, [x_Leads].[Realtor vs Customer] 
	, [x_Leads].[Other Builder] 
	, [x_Leads].[Why]
	, [x_Leads].IsRealtor__pc 
	, [x_Leads].Lost_Opportunity__c 
	, [x_Leads].What_Did_They_Purchase__c 
	, [Source] = 'Leads'
	, [x_Leads].[Created By]
	, [x_Leads].[ProfileId]
	,[x_Leads].AccountID
	,[x_Leads].ContactID
FROM [#Sale] [Sale]
	LEFT JOIN [#Leads] [x_Leads]
		ON [x_Leads].[OpportunityID] = [Sale].[OpportunityID]
			AND CAST([Sale].[SaleDateApproved] AS DATE) >= CAST([x_Leads].[Created Date Opportunity]  AS DATE)
			AND [x_Leads].[Opportunity Row Number Desc] = 1

-----------------------------------------------------------------------------------------------------------------------------------------
-- LEADS DATA - PURCHASED WITH REALTOR
-----------------------------------------------------------------------------------------------------------------------------------------
UNION ALL

SELECT  
DISTINCT
	  [x_Leads].[OpportunityID] 		
	, [x_Leads].[Opportunity Row Number] 
	,[x_Leads].[Account Row Number] 
	, [x_Leads].[Lead Source]									
	, [Sale].[SaleDivisionName] AS [Division]							
	, [x_Leads].[CommunityID]  											
	, [Sale].[SaleCommunityName] AS [Community]								
	, [x_Leads].[Created Month Opportunity] 	
	, [x_Leads].[Created Date Opportunity] 	
	, [x_Leads].[Email] 
	, [x_Leads].[Owner Name] 	
	, [x_Leads].[Owner Id]
	, [x_Leads].[Stage] 						
	, [x_Leads].[Connection Name]			
	, [x_Leads].[OSM Rating] 			
	, [x_Leads].[OSM Lead Source]			
	, [x_Leads].[Contact Name] 			
	, [x_Leads].[Contact Phone] 			
	, [x_Leads].[Agent Email]			
	, [x_Leads].[Coming Soon Communities]	
	, [x_Leads].[Active Communities] 	
	, [x_Leads].[Community Status] 			
	--------------------------------New Stuff 
	, [x_Leads].Lead_Ranking__c
	, [x_Leads].Lead_Ranking_Modified_Date__c 
	, [x_Leads].Lead_Reference_ID__c 
	, [x_Leads].Prospect_Rating__c 
	, [x_Leads].Agent_License_number__c 
	, [x_Leads].How_did_you_hear_about_us__c 
	, [x_Leads].Move_in_range__c 
	, [x_Leads].Price_Range__c 
	, [x_Leads].Reg_Card_Created_date__c 
	, [x_Leads].Reservation_No_Agent__C 
	, [x_Leads].I_Plan_to_purchase_a_home__C 
	, 'Purchased with a Realtor' AS [Metric]
	, [Sale].[SaleDateApproved]
	, 1 AS [Quantity]
	, NULL AS [Quote]
	, NULL AS [SaleAgentFullName]
	, NULL AS [SaleEarnestDepositAmount]
	, NULL AS [NetSalesPrice]
	, NULL AS [LotID]
	, NULL AS [SaleRealtorName]
	, NULL AS [SaleBuyerName]
	, NULL AS [IHC Sale]
	, [x_Leads].PersonMailingPostalCode
	, [x_Leads].PersonMailingStreet
	, [x_Leads].PersonMailingCity
	, [x_Leads].PersonMailingState
	, [x_Leads].PersonMailingStateCode
	, [x_Leads].DataLastRefreshed
	, [x_Leads].[Realtor vs Customer] 
	, [x_Leads].[Other Builder] 
	, [x_Leads].[Why]
	, [x_Leads].IsRealtor__pc 
	, [x_Leads].Lost_Opportunity__c 
	, [x_Leads].What_Did_They_Purchase__c 
	, [Source] = 'Leads'
	, [x_Leads].[Created By]
	, [x_Leads].[ProfileId]
	,[x_Leads].AccountID
	,[x_Leads].ContactID
FROM [#Sale] [Sale]
	LEFT JOIN [#Leads] [x_Leads]
		ON [x_Leads].[OpportunityID] = [Sale].[OpportunityID]
			AND CAST([Sale].[SaleDateApproved] AS DATE) >= CAST([x_Leads].[Created Date Opportunity]  AS DATE)
			AND [x_Leads].[Opportunity Row Number Desc] = 1
WHERE ( [Sale].[SaleRealtorName] IS NOT NULL AND [Sale].[SaleRealtorName] <> '' )