-- Databricks notebook source
-- MAGIC %md
-- MAGIC Importing Data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ClinicalDF = spark.read.options(delimiter = "|").csv("/FileStore/tables/clinicaltrial_2021.csv/",
-- MAGIC                        header = "true",
-- MAGIC                        inferSchema="true")
-- MAGIC ClinicalDF.show(10, truncate = False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ClinicalDF.createOrReplaceTempView("ClinicalView")

-- COMMAND ----------

select * from ClinicalView;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3>Question One </h3>
-- MAGIC Number of Studies

-- COMMAND ----------

--Display number of studies in dataset with count
select distinct(count(*)) as Number_of_Studies from ClinicalView; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3>Question Two</h3>
-- MAGIC Types of Studies in the
-- MAGIC dataset along with the frequencies of each type ordered from most frequent to least frequent

-- COMMAND ----------

--Select Type column and count of types.group by type and order by count in descending order
select Type,Count(*) as Count from ClinicalView
group by Type
order by Count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3>Question Three</h3>
-- MAGIC  Top 5 conditions with their frequencies.

-- COMMAND ----------

 -- Select the Conditions column and count the frequency of each condition
 --Group new results by conditions column and order by frequency of conditions
 --limit to top five results and order in descending
select trim(unnested.Conditions) as Conditions, count(*) as freq
from (
  select explode(split(Conditions, ',')) as Conditions
  from ClinicalView
) as unnested
group by unnested.Conditions
order by freq desc
limit 5


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3>Question Four</h3>
-- MAGIC 10 most common sponsors that are not pharmaceutical companies, along
-- MAGIC with the number of clinical trials they have sponsored.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Loading Pharma Df

-- COMMAND ----------

-- MAGIC %python
-- MAGIC PharmaDF = spark.read.options(delimeter = ",").csv("/FileStore/tables/pharma.csv/",
-- MAGIC                        header = "true",
-- MAGIC                        inferSchema="false")
-- MAGIC PharmaDF.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC PharmaDF.createOrReplaceTempView("PharmaView")

-- COMMAND ----------

select * from PharmaView

-- COMMAND ----------

--Select and count Sponsors column from ClinicalView
--Left anti join PharmaView.Grouped by sponsor and ordered in descending order
select cl.Sponsor as Sponsors, count(*) as Frequency
from ClinicalView cl
left anti join PharmaView p
on cl.Sponsor = p.Parent_Company
group by cl.Sponsor
order by Count(*) desc
limit (10)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3> Question Five</h3>

-- COMMAND ----------

--Select and count completion column where nested query meets required conditions
select Completion, count(*) 
from(select * from ClinicalView
where Status == 'Completed' and Completion like '%2021')
Group by Completion

-- COMMAND ----------

 -- Select the first 3 characters of the Completion column as Month
 -- Count the number of occurrences where query meets conditions and group by month
 ---- Assign a numerical value to each month for sorting purposes
SELECT SUBSTRING(Completion, 1, 3) AS Month, COUNT(*) AS count
FROM ClinicalView
WHERE Status = 'Completed' AND Completion LIKE '%2021'
GROUP BY Month
ORDER BY CASE Month
         WHEN 'Jan' THEN 1
         WHEN 'Feb' THEN 2
         WHEN 'Mar' THEN 3
         WHEN 'Apr' THEN 4
         WHEN 'May' THEN 5
         WHEN 'Jun' THEN 6
         WHEN 'Jul' THEN 7
         WHEN 'Aug' THEN 8
         WHEN 'Sep' THEN 9
         WHEN 'Oct' THEN 10
         WHEN 'Nov' THEN 11
         WHEN 'Dec' THEN 12
         END;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.functions import substring
-- MAGIC
-- MAGIC # Execute the query and store the results in a dataframe
-- MAGIC df = spark.sql("""
-- MAGIC     SELECT SUBSTRING(Completion, 1, 3) AS Month, COUNT(*) AS count
-- MAGIC     FROM ClinicalView
-- MAGIC     WHERE Status = 'Completed' AND Completion LIKE '%2021'
-- MAGIC     GROUP BY Month
-- MAGIC     ORDER BY CASE Month
-- MAGIC              WHEN 'Jan' THEN 1
-- MAGIC              WHEN 'Feb' THEN 2
-- MAGIC              WHEN 'Mar' THEN 3
-- MAGIC              WHEN 'Apr' THEN 4
-- MAGIC              WHEN 'May' THEN 5
-- MAGIC              WHEN 'Jun' THEN 6
-- MAGIC              WHEN 'Jul' THEN 7
-- MAGIC              WHEN 'Aug' THEN 8
-- MAGIC              WHEN 'Sep' THEN 9
-- MAGIC              WHEN 'Oct' THEN 10
-- MAGIC              WHEN 'Nov' THEN 11
-- MAGIC              WHEN 'Dec' THEN 12
-- MAGIC              END
-- MAGIC """).toPandas()
-- MAGIC
-- MAGIC # Plot the results using Pandas
-- MAGIC df.plot(kind='bar', x='Month', y='count', legend=None)
-- MAGIC plt.title('Number of Completed Clinical Trials in 2021 by Month')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Count')
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3>Additional Query</h3>
-- MAGIC Pharma companies with most primary offences

-- COMMAND ----------

SELECT Parent_Company, COUNT(DISTINCT Primary_Offense) AS Num_Primary_Offenses
FROM PharmaView
GROUP BY Parent_Company
ORDER BY Num_Primary_Offenses DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("""
-- MAGIC
-- MAGIC SELECT Parent_Company, COUNT(DISTINCT Primary_Offense) AS Num_Primary_Offenses
-- MAGIC FROM PharmaView
-- MAGIC GROUP BY Parent_Company
-- MAGIC ORDER BY Num_Primary_Offenses DESC
-- MAGIC LIMIT 10;
-- MAGIC
-- MAGIC
-- MAGIC """).toPandas()
-- MAGIC
-- MAGIC # Plot the results using Pandas
-- MAGIC df.plot(kind='bar', x='Parent_Company', y='Num_Primary_Offenses', legend=None)
-- MAGIC plt.title('Pharma companies with most primary offences')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Count')
-- MAGIC plt.show()

-- COMMAND ----------


