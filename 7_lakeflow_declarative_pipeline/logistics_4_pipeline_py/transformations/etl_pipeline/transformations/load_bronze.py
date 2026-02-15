from pyspark import pipelines as dp

@dp.table(name="prodcatalog_wd36.logistics_wd36.bronze_staff_data1")
def bronze_staff_data():

    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")
            .load("/Volumes/prodcatalog_wd36/logistics_wd36/datalake/staff/"))


@dp.table(name="prodcatalog_wd36.logistics_wd36.bronze_geotag_data1")
def bronze_geotag_data():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .load("/Volumes/prodcatalog_wd36/logistics_wd36/datalake/geotag/")
    )


@dp.table(name="prodcatalog_wd36.logistics_wd36.bronze_shipments_data1")
def bronze_shipments_data():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("inferColumnTypes", "true")
            .option("multiLine", "true")
            .load("/Volumes/prodcatalog_wd36/logistics_wd36/datalake/shipment/")
            .select(
                "shipment_id",
                "order_id",
                "source_city",
                "destination_city",
                "shipment_status",
                "cargo_type",
                "vehicle_type",
                "payment_mode",
                "shipment_weight_kg",
                "shipment_cost",
                "shipment_date"
            )
    )
