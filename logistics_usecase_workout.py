# Databricks notebook source
#Create a Spark Session Object
spark=spark.builder.getOrCreate()

# COMMAND ----------


# Read text file with delimiter

df1 = spark.read.option("mode", "PERMISSIVE").option("delimiter", ",").option("header", "true").option("inferSchema", "false").option("columnNameOfCorruptRecord","error_rec").format("csv").load("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/logistics_source1.txt").toDF("shipment_id","fname","lname","age","role")
df2 = spark.read.option("delimiter", ",").option("header", "true").option("inferSchema", "true").option("columnNameOfCorruptRecord","error_rec").format("csv").load("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/logistics_source2.txt").toDF("shipment_id","fname","lname","age","role","hub_location","vehicle_type")

# Write as CSV
#df.write.mode("overwrite").option("header", "true").csv("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/output_csv")
df1.show(10)
#or#
display(df1)
df1.printSchema()
print(df1.columns)
print(df1.dtypes)
display(df1.describe())
#or#
df1.describe().show()
print(df1.count())
df1.distinct().count()


df2.show(10)
#or#
display(df2)
df2.printSchema()
print(df2.columns)
print(df2.dtypes)
display(df2.describe())
#or#
df2.describe().show()
print(df2.count())
df2.distinct().count()



# COMMAND ----------

# Shipment IDs that appear in both master_v1 and master_v2
from pyspark.sql.functions import col
common_shipment_ids = df1.select("shipment_id").intersect(df2.select("shipment_id"))
display(common_shipment_ids)
display(common_shipment_ids.filter(common_shipment_ids.shipment_id.isNotNull()))
common_shipment_ids.count()

# Shipment IDs that appear in either master_v1 or master_v2)

# Records where shipment_id is non-numeric
non_numeric_shipment_id_df1 = df1.filter(~df1.shipment_id.rlike("^[0-9]+$"))
display(non_numeric_shipment_id_df1)
non_numeric_shipment_id_df2 = df2.filter(~df2.shipment_id.rlike("^[0-9]+$"))
display(non_numeric_shipment_id_df2)

# Records where age is not an integer
non_integer_age_df1 = df1.filter(~df1.age.rlike("^[0-9]+$"))
display(non_integer_age_df1)
non_integer_age_df2 = df2.filter(~df2.age.rlike("^[0-9]+$"))
display(non_integer_age_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Combining Data + Schema Merging (Structuring)
# MAGIC - Read both files without enforcing schema
# MAGIC - Align them into a single canonical schema: shipment_id, first_name, last_name, age, role, hub_location, vehicle_type, data_source
# MAGIC - Add data_source column with values as: system1, system2 in the respective dataframes

# COMMAND ----------

# MAGIC %sh
# MAGIC #recursively remove files & directory if non empty
# MAGIC rm -rf "/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/schemMerging/"

# COMMAND ----------

from pyspark.sql.functions import *
set1 =spark.read.csv("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/logistics_source1.txt",header=True)
set2 =spark.read.csv("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/logistics_source2.txt",header=True)
#set1.printSchema()
#set2.printSchema()
set1.withColumn("source", lit("system1")).withColumn("load_Dt",current_timestamp()).write.mode("overwrite").parquet("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/schemMerging")
set2.withColumn("source", lit("system2")).withColumn("load_Dt",current_timestamp()).write.mode("append").parquet("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/schemMerging")
df_merged=spark.read.parquet("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/schemMerging",mergeSchema=True)
df_merged.printSchema()
df_merged.show()
df_merged.count()

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.types import IntegerType
# MAGIC
# MAGIC df_merged_casted = df_merged.withColumn("shipment_id", col("shipment_id").cast(IntegerType())) \
# MAGIC                             .withColumn("age", col("age").cast(IntegerType()))
# MAGIC
# MAGIC ###or
# MAGIC cast_map = {"shipment_id": IntegerType(), "age": IntegerType()}
# MAGIC
# MAGIC df_merged_casted = df_merged.select(
# MAGIC     *[
# MAGIC         col(c).cast(cast_map[c]).alias(c) if c in cast_map else col(c)
# MAGIC         for c in df_merged.columns
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC display(df_merged_casted)
# MAGIC df_merged_casted.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cleansing, Scrubbing:
# MAGIC
# MAGIC Cleansing (removal of unwanted datasets)
# MAGIC
# MAGIC - Mandatory Column Check - Drop any record where any of the following columns is NULL:shipment_id, role
# MAGIC - Name Completeness Rule - Drop records where both of the following columns are NULL: first_name, last_name
# MAGIC - Join Readiness Rule - Drop records where the join key is null: shipment_id
# MAGIC
# MAGIC Scrubbing (convert raw to tidy)
# MAGIC
# MAGIC - Age Defaulting Rule - Fill NULL values in the age column with: -1
# MAGIC - Vehicle Type Default Rule - Fill NULL values in the vehicle_type column with: UNKNOWN
# MAGIC - Invalid Age Replacement - Replace the following values in age: "ten" to -1 "" to -1
# MAGIC - Vehicle Type Normalization - Replace inconsistent vehicle types: truck to LMV bike to TwoWheeler

# COMMAND ----------

df_merged.filter("age='ten%'").show()

# COMMAND ----------

df_merged.createOrReplaceTempView("merged_vw")

df_dsl=df_merged.na.drop(subset=["shipment_id"]).na.drop(subset=["shipment_id","role"]).na.drop(subset=["first_name","last_name"],how='all').withColumn("age", when(col("age").isNull(), -1).when(col("age") == "ten", "-1").when(col("age") == "", -1).otherwise(col("age"))).withColumn("vehicle_type", when(col("vehicle_type").isNull(), "UNKNOWN").when(col("vehicle_type") == "truck", "LMV").when(col("vehicle_type") == "bike", "TwoWheeler").otherwise(col("vehicle_type")))



df_sql=spark.sql("""SELECT 
    shipment_id,
    first_name,
    last_name,
    CASE 
        WHEN age IS NULL THEN -1
        WHEN age = 'ten' THEN -1
        ELSE age
    END AS age,
    role,
    source,
    hub_location,
    CASE 
        WHEN vehicle_type = 'truck' THEN 'LMV'
        WHEN vehicle_type = 'bike' THEN 'TwoWheeler'
        ELSE COALESCE(vehicle_type, 'Unknown')
    END AS vehicle_type
FROM merged_vw
WHERE shipment_id IS NOT NULL
  AND role IS NOT NULL
  AND (first_name IS NOT NULL OR last_name IS NOT NULL)
 """)
display(df_dsl)
display(df_sql)
#df_dsl.show(df_dsl.count())
print(df_dsl.count())

#df_sql.show(df_sql.count())
print(df_sql.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Standardization, De-Duplication and Replacement / Deletion of Data to make it in a usable format
# MAGIC Creating shipments Details data Dataframe creation
# MAGIC
# MAGIC Create a DF by Reading Data from logistics_shipment_detail.json
# MAGIC As this data is a clean json data, it doesn't require any cleansing or scrubbing.

# COMMAND ----------

df_json = spark.read.option("multiline", "true").json("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/logistics_shipment_detail_3000.json")
display(df_json)
df_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Standardizations:<br>
# MAGIC
# MAGIC 1. Add a column<br> 
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>: domain as 'Logistics',  current timestamp 'ingestion_timestamp' and 'False' as 'is_expedited'
# MAGIC 2. Column Uniformity: 
# MAGIC role - Convert to lowercase<br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC vehicle_type - Convert values to UPPERCASE<br>
# MAGIC Source Files: DF of logistics_shipment_detail_3000.json
# MAGIC hub_location - Convert values to initcap case<br>
# MAGIC Source Files: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC 3. Format Standardization:<br>
# MAGIC Source Files: DF of logistics_shipment_detail_3000.json<br>
# MAGIC Convert shipment_date to yyyy-MM-dd<br>
# MAGIC Ensure shipment_cost has 2 decimal precision<br>
# MAGIC 4. Data Type Standardization<br>
# MAGIC Standardizing column data types to fix schema drift and enable mathematical operations.<br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2) <br>
# MAGIC age: Cast String to Integer<br>
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC shipment_weight_kg: Cast to Double<br>
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC is_expedited: Cast to Boolean<br>
# MAGIC 5. Naming Standardization <br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC Rename: first_name to staff_first_name<br>
# MAGIC Rename: last_name to staff_last_name<br>
# MAGIC Rename: hub_location to origin_hub_city<br>
# MAGIC 6. Reordering columns logically in a better standard format:<br>
# MAGIC Source File: DF of Data from all 3 files<br>
# MAGIC shipment_id (Identifier), staff_first_name (Dimension)staff_last_name (Dimension), role (Dimension), origin_hub_city (Location), shipment_cost (Metric), ingestion_timestamp (Audit)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_json = spark.read.option("multiline", "true")\
    .json("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/logistics_shipment_detail_3000.json")\
    .withColumn("sourcefile", regexp_extract(col("_metadata.file_path"), r"([^/]+)$", 1)) \
    .withColumn("domain",lit("logistics")) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("is_expedited", lit("false"))  # Use boolean literal, not string

display(df_json)
df_json.count()
df_json_standardized = df_json\
    .withColumn("vehicle_type", upper(col("vehicle_type")))\
    .withColumn("shipment_date", to_date(col("shipment_date"), "yy-MM-dd"))\
    .withColumn("shipment_cost", col("shipment_cost").cast("decimal(10,2)"))\
    .withColumn("is_expedited", col("is_expedited").cast(BooleanType()))

display(df_json_standardized)
df_json_standardized.printSchema()

#DSL
df_dsl_standardized = df_dsl.withColumn("role",lower(col("role")))\
    .withColumn("hub_location",initcap(col("hub_location")))\
    .withColumn("age",col("age").cast("int"))\
    .withColumnRenamed("first_name","staff_first_name")\
    .withColumnRenamed("last_name","staff_last_name")\
        .withColumnRenamed("hub_location","origin_hub_city")\
        .withColumn("ingestion_timestamp",current_timestamp())
df_dsl_standardized.printSchema()
display(df_dsl_standardized)   
print(df_dsl_standardized.count())     

#SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ###Deduplication:
# MAGIC
# MAGIC Apply Record Level De-Duplication
# MAGIC Apply Column Level De-Duplication (Primary Key Enforcement)

# COMMAND ----------

df_dsl_deduplicate=df_dsl_standardized.dropDuplicates().dropDuplicates(["shipment_id"])
df_json_deduplicate=df_json_standardized.dropDuplicates().dropDuplicates(["order_id"])
print(df_dsl_deduplicate.count())
print(df_json_deduplicate.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Data Enrichment - Detailing of data
# MAGIC Makes your data rich and detailed <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Adding of Columns (Data Enrichment)
# MAGIC *Creating new derived attributes to enhance traceability and analytical capability.*
# MAGIC
# MAGIC **1. Add Audit Timestamp (`load_dt`)**
# MAGIC Source File: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** We need to track exactly when this record was ingested into our Data Lakehouse for auditing purposes.
# MAGIC * **Action:** Add a column `load_dt` using the function `current_timestamp()`.
# MAGIC
# MAGIC **2. Create Full Name (`full_name`)**
# MAGIC Source File: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** The reporting dashboard requires a single field for the driver's name instead of separate columns.
# MAGIC * **Action:** Create `full_name` by concatenating `first_name` and `last_name` with a space separator.
# MAGIC * **Result:** "Rajesh" + " " + "Kumar" -> **"Rajesh Kumar"**
# MAGIC
# MAGIC **3. Define Route Segment (`route_segment`)**
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC * **Scenario:** The logistics team wants to analyze performance based on specific transport lanes (Source to Destination).
# MAGIC * **Action:** Combine `source_city` and `destination_city` with a hyphen.
# MAGIC * **Result:** "Chennai" + "-" + "Pune" -> **"Chennai-Pune"**
# MAGIC
# MAGIC **4. Generate Vehicle Identifier (`vehicle_identifier`)**
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC * **Scenario:** We need a unique tracking code that immediately tells us the vehicle type and the shipment ID.
# MAGIC * **Action:** Combine `vehicle_type` and `shipment_id` to create a composite key.
# MAGIC * **Result:** "Truck" + "_" + "500001" -> **"Truck_500001"**

# COMMAND ----------

#from pyspark.sql.functions import *


df_dsl_enrich=df_dsl_deduplicate.withColumn("load_dt",current_timestamp()).withColumn("full_name",concat(col("staff_first_name"),lit(" "),col("staff_last_name"))).drop("staff_first_name","staff_last_name")

display(df_dsl_enrich)

df_json_enrich=df_json_deduplicate.withColumn("route_Segement",concat(col("source_city"),lit("-"),col("destination_city"))).withColumn("vehicle_identifier",concat(col("vehicle_type"),lit("_"),col("shipment_id")))

display(df_json_enrich)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Deriving of Columns (Time Intelligence)
# MAGIC *Extracting temporal features from dates to enable period-based analysis and reporting.*<br>
# MAGIC Source File: logistics_shipment_detail_3000.json<br>
# MAGIC **1. Derive Shipment Year (`shipment_year`)**
# MAGIC * **Scenario:** Management needs an annual performance report to compare growth year-over-year.
# MAGIC * **Action:** Extract the year component from `shipment_date`.
# MAGIC * **Result:** "2024-04-23" -> **2024**
# MAGIC
# MAGIC **2. Derive Shipment Month (`shipment_month`)**
# MAGIC * **Scenario:** Analysts want to identify seasonal peaks (e.g., increased volume in December).
# MAGIC * **Action:** Extract the month component from `shipment_date`.
# MAGIC * **Result:** "2024-04-23" -> **4** (April)
# MAGIC
# MAGIC **3. Flag Weekend Operations (`is_weekend`)**
# MAGIC * **Scenario:** The Operations team needs to track shipments handled during weekends to calculate overtime pay or analyze non-business day capacity.
# MAGIC * **Action:** Flag as **'True'** if the `shipment_date` falls on a Saturday or Sunday.
# MAGIC
# MAGIC **4. Flag shipment status (`is_expedited`)**
# MAGIC * **Scenario:** The Operations team needs to track shipments is IN_TRANSIT
# MAGIC  or DELIVERED.
# MAGIC * **Action:** Flag as **'True'** if the `shipment_status` IN_TRANSIT or DELIVERED.

# COMMAND ----------


df_json_time_int=df_json_enrich.withColumn("shipment_year",year(col("shipment_date"))).withColumn("shipment_month",month(col("shipment_date"))).withColumn("shipment_day",dayofweek(col("shipment_date"))).withColumn("is_weekend",when((dayofweek(col("shipment_date")) == 1) | (dayofweek(col("shipment_date")) == 7) ,lit("True")).otherwise(lit("False"))).withColumn("is_expedited",when((col("shipment_status") == "IN_TRANSIT") | (col("shipment_status") == "DELIVERED"),lit("True")).otherwise(lit("False")))
display(df_json_time_int)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Enrichment/Business Logics (Calculated Fields)
# MAGIC *Deriving new metrics and financial indicators using mathematical and date-based operations.*<br>
# MAGIC Source File: logistics_shipment_detail_3000.json<br>
# MAGIC
# MAGIC **1. Calculate Unit Cost (`cost_per_kg`)**
# MAGIC * **Scenario:** The Finance team wants to analyze the efficiency of shipments by determining the cost incurred per unit of weight.
# MAGIC * **Action:** Divide `shipment_cost` by `shipment_weight_kg`.
# MAGIC * **Logic:** `shipment_cost / shipment_weight_kg`
# MAGIC
# MAGIC **2. Track Shipment Age (`days_since_shipment`)**
# MAGIC * **Scenario:** The Operations team needs to monitor how long it has been since a shipment was dispatched to identify potential delays.
# MAGIC * **Action:** Calculate the difference in days between the `current_date` and the `shipment_date`.
# MAGIC * **Logic:** `datediff(current_date(), shipment_date)`
# MAGIC
# MAGIC **3. Compute Tax Liability (`tax_amount`)**
# MAGIC * **Scenario:** For invoicing and compliance, we must calculate the Goods and Services Tax (GST) applicable to each shipment.
# MAGIC * **Action:** Calculate 18% GST on the total `shipment_cost`.
# MAGIC * **Logic:** `shipment_cost * 0.18`

# COMMAND ----------

df_json_time_int.withColumn
