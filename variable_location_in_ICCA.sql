/**************************************
** File: variable_location_in_ICCA.sql
** Author: C.McWilliams
** Created: 25/07/2019
**
** Description: This script illustrates some useful methods for locating variables in the back end of the Philips ICCA system.
**				Comments inline explain usage. On our system the queries are run on the Reporting database. It is important not
**				to clog the server so queries should be supervised and cancelled if they overrun. It is easy to write long queries
**				on this database!
**				In MSql Server run individual queries by selecting the text and pressing 'F5'.
**
**				Note that the following examples are only illustrative of methods for approaching variable location, the actual
**				configuration of different ICCA systems will mean that the queries need some adapting to be re-used.
**************************************/

-- The ICCA reporting database has a star schema with definition and fact tables. 
-- Tables beginning 'D_' are definition tables and define certain concepts (e.g. D_Intervetions).
-- The substantive patient data about these concepts are stored in fact tables (e.g. PtAssessment). 

-- Set database
USE CISReportingDB

-- Table types can be viewed like this:
SELECT * FROM dbo.M_TableType

-- In general we search for variables in the D_Intervention table by pattern matching on the longLabel.
-- For example, to find heart rate we search:
SELECT TOP 100 * FROM D_Intervention WHERE longLabel LIKE '%heart rate%'  -- this returns 14 interventions, of which 4 are relevant (by manual inspection). 
-- It is also possible to search on the shortLabel, which is usually an abbreviated name for the intervention:
SELECT TOP 100 * FROM D_Intervention WHERE shortLabel LIKE '% hr %'  
-- The above search is less useful (returns >100 results) because hr is also short for 'hour' and is included in many labels.
-- In general it works well to search for interventions on the longLabel.

-- However, for some interventions a search on the shortLabel performs better.
-- For exmple, the following search does not find non-invasive blood pressure:
SELECT TOP 100 * FROM D_Intervention WHERE longLabel LIKE '%blood pressure%'
-- Whereas a search on the shortLabel abbreviation finds the five NBP interventions:
SELECT TOP 100 * FROM D_Intervention WHERE shortLabel LIKE '%nbp%'
-- The following interventionIds are defined for NBP: 3363, 3779, 4001, 7794, 21039

-- It is often informative to look at variable names in the front-end of ICCA to generate search terms. 
-- Although this is not 100% reliable. In particualr it only captures the current system state, and
-- the same intervention may have been encoded differently in the past.

-- Having located the required intervention it is necessary to find the associated attribute that contains the desired data.
-- Some intervention have only a single attribute, while others have mutiple.
-- To locate attributes we must link through the associated fact table for the given intervention.
-- For example, to find the non-invasive blood pressure attributes:
SELECT TOP 100 DI.interventionId as interventionId, MIN(DI.longLabel) as longLabel, DA.attributeId as attributeId, MIN(DA.shortLabel) as shortLabel, MIN(DA.conceptLabel) as conceptLabel, COUNT(DISTINCT(encounterId)) as frequency 
FROM D_Attribute DA
INNER JOIN PtAssessment P
ON P.attributeId=DA.attributeId
INNER JOIN D_Intervention DI
ON DI.interventionId=P.interventionId
WHERE DI.interventionId in (3363, 3779, 4001, 7794, 21039) AND P.clinicalUnitId=5
GROUP BY DI.interventionId, DA.attributeId 
ORDER BY frequency DESC
-- The above query finds the interventions and attributes that are in use in the PtAssessment table,
-- And counts the frequency of occurence (number of ICU stays with a measurement of that intervention-attribute pair).
-- It shows us that the interventions for Non-Invasive BP have separate attributes for Mean, Systolic and Diastolic values.
-- There are also attributes such as Site and Status whcih contain extra information but are recorded less frequently. 
-- We also find that only 3 of 5 interventions for NBP have ever been used on unit 5 (GICU). 

-- We can use the output of the above query to e.g. gathering all attributes for Mean NBP.
-- Then if we wanted to extract all measurements of Mean NBP we could run the query:
SELECT * FROM PtAssessment WHERE interventionId in (3363, 7794, 21039) and attributeId in (10660, 27260, 27264, 43381, 43388)
-- The data values are stored in different columns depedning on the type of data (e.g. numeric or string).

-- In general the above procedure for variable location is robust i.e.: 
--		> Find relevant interventionId for the variable you want ot locate. 
--		> Then locate the relevant attributes by linking to the associated fact table.
--
-- However, in some cases there are many intervetnions to manually inspect. 
-- For example there following search returns 1694 interventions for the medication Furosemide:
SELECT * FROM D_Intervention WHERE longLabel like '%furosemide%'

-- We can search for all of these in the PtMedication fact table to see which ones are in use (and with which attributes):
SELECT DI.interventionId as interventionId, MIN(DI.longLabel) as longLabel, DA.attributeId as attributeId, MIN(DA.shortLabel) as shortLabel, MIN(DA.conceptLabel) as conceptLabel, COUNT(DISTINCT(encounterId)) as frequency 
FROM D_Attribute DA
INNER JOIN PtMedication P
ON P.attributeId=DA.attributeId
INNER JOIN D_Intervention DI
ON DI.interventionId=P.interventionId
WHERE DI.interventionId in (select interventionId from D_Intervention where longLabel like '%furosemide%') AND P.clinicalUnitId=5
GROUP BY DI.interventionId, DA.attributeId 
ORDER BY frequency DESC
-- The query returns almost 2000 intervention-attribute pairs for furosemide. 
-- Harmonising these data for secondary use requires careful consideration and depends on the task at hand. 
-- It can again be informative to look in the ICCA front end to see how the intervention is encoded there.
-- We may decide that we are only interested in Dose inforamtion and can therefore discard all other attributes:
SELECT DI.interventionId as interventionId, MIN(DI.longLabel) as longLabel, DA.attributeId as attributeId, MIN(DA.shortLabel) as shortLabel, MIN(DA.conceptLabel) as conceptLabel, COUNT(DISTINCT(encounterId)) as frequency 
FROM D_Attribute DA
INNER JOIN PtMedication P
ON P.attributeId=DA.attributeId
INNER JOIN D_Intervention DI
ON DI.interventionId=P.interventionId
WHERE DI.interventionId in (select interventionId from D_Intervention where longLabel like '%furosemide%') AND P.clinicalUnitId=5 AND DA.shortLabel='Dose'
GROUP BY DI.interventionId, DA.attributeId 
ORDER BY frequency DESC

-- Restricting the extract to only dosage attributes reduces the number of intervention-attribute pairs to 207.
-- These 207 data types can be further harmonised depending on how we want to use them.
-- For example, we may want to combine all furosemide infusions into a single variable, and all
-- bolus/enteral administrations into another. 
-- This can simply be done by combining all Dose values given in 'mg/hr', and all others given in 'mg'. 
