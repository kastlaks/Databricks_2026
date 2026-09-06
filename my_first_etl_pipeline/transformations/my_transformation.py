from pyspark import pipelines as dp
from pyspark.sql.functions import current_date,lit

@dp.table
def get_df():
    df=spark.range(100).withColumn("dt",current_date()).withColumn("user",lit("sdpuser"))
    return df