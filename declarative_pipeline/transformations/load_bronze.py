from pyspark import pipelines as dp



#base_path="/Volumes/catalog_wd37/logistics_sdp/datalake"
base_path=spark.conf.get("src_path")

@dp.table(name="catalog_wd37.logistics_sdp.bronze_staff_data")
def load_staff():
    df=(spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.inferColumnTypes",True)                
        .option("cloudFiles.schemaEvolutionMode","addNewColumns")
        .load(f"{base_path}/staff"))
    return df


@dp.table(name="catalog_wd37.logistics_sdp.bronze_geotag_data")
def load_geo_data():
    df=(spark.readStream.format("cloudFiles")
       .option("cloudFiles.format","csv")
       .option("cloudFiles.inferColumnTypes",True)
       .load(f"{base_path}/geotag")
    )
    return df


@dp.table(name="catalog_wd37.logistics_sdp.bronze_shipment_data")
def load_shipment_data():
    df=(spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","json")
        .option("cloudFiles.inferColumnTypes",True)
        .option("multiline",True)
        .load(f"{base_path}/shipment").select("shipment_id", "order_id", "source_city", "destination_city",
                "shipment_status", "cargo_type", "vehicle_type", "payment_mode",
                "shipment_weight_kg", "shipment_cost", "shipment_date")
    )
    return df
                