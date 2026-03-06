from pyspark.sql.functions import *
from pyspark import pipelines as dp

catalog_name = "abccompany"
schema_name = "fixeddata"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

############################## Geo Heirachy table ###################
@dp.materialized_view()
def sl_geo_hierarchy():
    df = spark.table("bs_geo_hierarchy")
    return (
        df
        #BASIC DATA QUALITY
        .filter(col("geo_code").isNotNull())
        .filter(col("geo_level").isin("TERR","DIST","REGN","NATION"))

        #normalize text
        .withColumn("geo_code", upper(trim(col("geo_code"))))
        .withColumn("geo_level", upper(trim(col("geo_level"))))
        .withColumn("geo_name", trim(col("geo_name")))
        .withColumn("parent_geo_code",
                    when(col("parent_id") == "-",None)
                    .otherwise(upper(trim(col("parent_id"))))
                    )
        # ✅ drop raw column
        .drop("parent_id")
    )

@dp.materialized_view()
def sl_geo_hierarchy_resolved():
    base = spark.table("sl_geo_hierarchy")

    
    terr = base.filter(col("geo_level") == "TERR") \
               .select(
                   col("geo_code").alias("territory_code"),
                   col("geo_name").alias("territory_name"),
                   col("parent_geo_code").alias("district_code")
               )

    dist = base.filter(col("geo_level") == "DIST") \
               .select(
                   col("geo_code").alias("district_code"),
                   col("geo_name").alias("district_name"),
                   col("parent_geo_code").alias("region_code")
               )
    
    regn = base.filter(col("geo_level") == "REGN") \
               .select(
                   col("geo_code").alias("region_code"),
                   col("geo_name").alias("region_name"),
                   col("parent_geo_code").alias("nation_code")
               )

    natn = base.filter(col("geo_level") == "NATION") \
               .select(
                   col("geo_code").alias("nation_code"),
                   col("geo_name").alias("nation_name")
               )

    return (
        terr
        .join(dist, "district_code", "left")
        .join(regn, "region_code", "left")
        .join(natn, "nation_code", "left")
    )

############################## Market Definition table ###################

@dp.materialized_view()
def sl_market_definition():
    df = spark.table("bs_market_definition")
    return (
        df
        #BASIC DATA QUALITY
        .filter(col("MKT_CD").isNotNull())
        .filter(col("MKT_NM").isNotNull())

        #normalize text
        .select(
            upper(trim(col("MKT_CD"))).alias("market_code"),
            upper(trim(col("MKT_NM"))).alias("market_name")
        )
        .dropDuplicates(["market_code"])
    )
############################## Prescriber table ###################
@dp.materialized_view()
def sl_prescriber():
    df = spark.table("bs_prescriber")
    return (
            df
            .select(
                upper(trim(col("prescriber_id"))).alias("prescriber_id"),
                initcap(trim(col("first_name"))).alias("first_name"),
                initcap(trim(col("last_name"))).alias("last_name"),
                initcap(trim(col("preffered_name"))).alias("preferred_name"),
                upper(trim(col("person_specialty"))).alias("specialty")
            )
            .filter(col("prescriber_id").isNotNull())
            .dropDuplicates(["prescriber_id"])
        )
############################## Prescriber Address table ###################
@dp.materialized_view()
def sl_prescriber_address():
    df = spark.table("bs_prescriber_address")
    return (
            df
            .select(
                upper(trim(col("person_code"))).alias("prescriber_id"),
                trim(col("address_1")).alias("address_1"),
                trim(col("address_2")).alias("address_2"),
                initcap(trim(col("city"))).alias("city"),
                upper(trim(col("state"))).alias("state"),
                lpad(trim(col("zip_code").cast("string")), 5, "0").alias("zip_code")
            )
            
            .filter(col("prescriber_id").isNotNull())
            .filter(col("zip_code").isNotNull())
            .dropDuplicates(["prescriber_id"])
    )

############################## zip to terr table ###################
@dp.materialized_view()
def sl_zip_to_terr():
    df = spark.table("bs_zip_to_terr")
    return (
        df
        .select(
            lpad(trim(col("zip_code").cast("string")), 5, "0").alias("zip_code"),
            upper(trim(col("territory_id"))).alias("territory_code")
        )
        .filter(col("zip_code").isNotNull())
        .filter(col("territory_code").isNotNull())
        .dropDuplicates(["zip_code", "territory_code"])
    )

############################## product master table ###################
@dp.materialized_view()
def sl_product_master():
    prod = spark.table("bs_product_master")
    mkt  = spark.table("sl_market_definition")

    # normalize product fields
    prod_clean = (
        prod
        .select(
            upper(trim(col("market_cd"))).alias("market_code"),
            trim(col("prd_pack_id").cast("string")).alias("prd_pack_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("strength")).alias("strength"),
            trim(col("brand_id").cast("string")).alias("brand_id"),
            upper(trim(col("brand_name"))).alias("brand_name"),
            upper(trim(col("prod_mnf"))).alias("prod_mnf")
        )
        .filter(col("prd_pack_id").isNotNull())
        .dropDuplicates(["prd_pack_id"])
    )
    
    return (
        prod_clean
        .join(mkt, on="market_code", how="left")
    )

############################## sales table ###################
@dp.materialized_view()
def sl_sales():

    df = spark.table("bs_sales")

    # Normalize key columns
    base = (
        df
        .select(
            upper(trim(col("psbr_id"))).alias("prescriber_id"),
            trim(col("prod_id").cast("string")).alias("product_id"),
            *df.columns[2:]  # all month columns
        )
        .filter(col("prescriber_id").isNotNull())
        .filter(col("product_id").isNotNull())
    )

    # Build one DataFrame per month, then union
    month_dfs = []

    for m in range(1, 13):
        month_df = (
            base
            .select(
                col("prescriber_id"),
                col("product_id"),
                lit(m).alias("month_num"),

                col(f"MONTH_{m}_NRX_UNIT").cast("int").alias("n_rx_unit"),
                col(f"MONTH_{m}_NRX_VOL").cast("int").alias("n_rx_vol"),
                col(f"MONTH_{m}_NRX_DOLLAR").cast("double").alias("n_rx_dollar"),

                col(f"MONTH_{m}_RRX_UNIT").cast("int").alias("r_rx_unit"),
                col(f"MONTH_{m}_RRX_VOL").cast("int").alias("r_rx_vol"),
                col(f"MONTH_{m}_RRX_DOLLAR").cast("double").alias("r_rx_dollar"),

                col(f"MONTH_{m}_TRX_UNIT").cast("int").alias("t_rx_unit"),
                col(f"MONTH_{m}_TRX_VOL").cast("int").alias("t_rx_vol"),
                col(f"MONTH_{m}_TRX_DOLLAR").cast("double").alias("t_rx_dollar")
            )
        )

        month_dfs.append(month_df)

    # Union all months
    sales_long = month_dfs[0]
    for df_m in month_dfs[1:]:
        sales_long = sales_long.unionByName(df_m)

    return sales_long











