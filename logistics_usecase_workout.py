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
set1.withColumn("source", lit("system1")).write.mode("overwrite").parquet("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/schemMerging")
set2.withColumn("source", lit("system2")).write.mode("append").parquet("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/schemMerging")
df_merged=spark.read.parquet("/Volumes/izwd37dev/wd37db/rawdatta/BB2/logistics_use_case/schemMerging",mergeSchema=True)
df_merged.printSchema()
df_merged.show()
df_merged.count()

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.types import IntegerType
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
