from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dp.materialized_view(name="catalog_wd37.logistics_sdp.gold_agg_shipment_stats")
def gold_shiment_stats():
    df=(spark.read.table("catalog_wd37.logistics_sdp.silver_shipment").groupBy("source_city")
        .agg(
            sum("shipment_cost_clean").alias("total_cost"),
            count("shipment_id").alias("total_shipments"),
            avg("shipment_weight_clean").alias("avg_weight")
        )
    )
    return df

@dp.materialized_view(name="catalog_wd37.logistics_sdp.gold_staff_geodetail")
def gold_staff_stats(): 
    df_staff = spark.read.table("catalog_wd37.logistics_sdp.silver_staff")
    df_geo = spark.read.table("catalog_wd37.logistics_sdp.silver_geotag")
    
    return (
        df_staff.join(df_geo,
            df_staff.hub_location == df_geo.city_name, 
            "inner")
        .select(
            df_staff["*"],
            df_geo["latitude"],
            df_geo["longitude"]
        )
    )
