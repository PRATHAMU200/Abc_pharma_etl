from pyspark.sql.functions import *
from pyspark import pipelines as dp
from pyspark.sql.window import Window

catalog_name = "abccompany"
schema_name = "fixeddata"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# “What % of total TRx does each brand/product contribute in a market?”

@dp.materialized_view()
def g_market_share():
    sales = spark.table("sl_sales")
    prod  = spark.table("sl_product_master")

    # ---------------------------------------------------
    # 1) Aggregate product-level TRx (annual)
    # ---------------------------------------------------
    product_trx = (
        sales
        .groupBy("product_id")
        .agg(
            sum("t_rx_unit").alias("product_trx_unit"),
            sum("t_rx_vol").alias("product_trx_vol"),
            sum("t_rx_dollar").alias("product_trx_dollar")
        )
    )

    # ---------------------------------------------------
    # 2) Attach market & product attributes
    # ---------------------------------------------------
    product_market = (
        product_trx
        .join(
            prod,
            product_trx.product_id == prod.prd_pack_id,
            "left"
        )
        .select(
            col("market_code"),
            col("market_name"),
            col("product_id"),
            col("product_name"),
            col("prod_mnf"),
            col("product_trx_unit"),
            col("product_trx_vol"),
            col("product_trx_dollar")
        )
    )

    # ---------------------------------------------------
    # 3) Compute total market TRx
    # ---------------------------------------------------
    market_trx = (
        product_market
        .groupBy("market_code", "market_name")
        .agg(
            sum("product_trx_unit").alias("market_trx_unit"),
            sum("product_trx_vol").alias("market_trx_vol"),
            sum("product_trx_dollar").alias("market_trx_dollar")
        )
    )

    # ---------------------------------------------------
    # 4) Calculate market share %
    # ---------------------------------------------------
    return (
        product_market
        .join(market_trx, ["market_code", "market_name"], "left")
        .withColumn(
            "market_share_pct",
            round(col("product_trx_unit") / col("market_trx_unit") * 100, 2)
        )
        .withColumn(
            "market_share_vol",
            round(col("product_trx_vol") / col("market_trx_vol") * 100, 2)
        )
        .withColumn(
            "market_share_dollar",
            round(col("product_trx_dollar") / col("market_trx_dollar") * 100, 2)
        )
        #.orderBy("market_code", desc("market_share_unit"))
    )

# Find out the Sales trend of the Products (at Brand level) for last 4 quarters
@dp.materialized_view(
    name="g_brand_sales_trend_qtr",
    comment="Gold: Brand-level TRx units trend across last 4 quarters (Q1-Q4)"
)
def g_brand_sales_trend_qtr():

    sales = spark.table("sl_sales")
    prod  = spark.table("sl_product_master")

    # 1) Attach brand to each sales row (product_id -> prd_pack_id)
    sales_brand = (
        sales
        .join(prod, sales.product_id == prod.prd_pack_id, "left")
        .select(
            col("brand_name"),
            col("prod_mnf"),
            col("month_num"),
            col("t_rx_unit").alias("trx_unit"),
            col("t_rx_vol").alias("trx_vol"),
            col("t_rx_dollar").alias("trx_dollar")
        )
        .filter(col("brand_name").isNotNull())
    )

    # 2) Convert month_num -> quarter (Q1..Q4)
    sales_qtr = (
        sales_brand
        .withColumn(
            "quarter",
            when(col("month_num").between(1, 3),  lit("Q1"))
            .when(col("month_num").between(4, 6),  lit("Q2"))
            .when(col("month_num").between(7, 9),  lit("Q3"))
            .when(col("month_num").between(10,12), lit("Q4"))
            .otherwise(lit(None))
        )
        .filter(col("quarter").isNotNull())
    )

    # 3) Aggregate to Brand x Quarter
    return (
        sales_qtr
        .groupBy("brand_name", "quarter","prod_mnf")
        .agg(
            sum("trx_unit").alias("trx_unit"),
            sum("trx_vol").alias("trx_vol"),
            sum("trx_dollar").alias("trx_dollar")
        )
        .orderBy("brand_name", "quarter")
    )

#Calculate the sales of a product by specialty type
@dp.materialized_view(
    name="g_product_sales_by_specialty",
    comment="Gold: Product sales (TRx units) by prescriber specialty"
)
def g_product_sales_by_specialty():

    sales = spark.table("sl_sales")
    pres  = spark.table("sl_prescriber")
    prod  = spark.table("sl_product_master")

    # 1) Join sales -> prescriber to get specialty
    sales_with_spec = (
        sales
        .join(pres, on="prescriber_id", how="left")
        .filter(col("specialty").isNotNull())
    )

    # 2) Join to product master to get product/brand/market attributes
    sales_enriched = (
        sales_with_spec
        .join(prod, sales_with_spec.product_id == prod.prd_pack_id, "left")
        .select(
            col("market_code"),
            col("market_name"),
            col("brand_name"),
            col("prod_mnf"),
            col("product_id"),
            col("product_name"),
            col("specialty"),
            col("t_rx_unit").alias("trx_unit"),
            col("t_rx_vol").alias("trx_vol"),
            col("t_rx_dollar").alias("trx_dollar")
        )
        .filter(col("product_name").isNotNull())
    )

    # 3) Aggregate: Product x Specialty
    return (
        sales_enriched
        .groupBy(
            "market_code", "market_name",
            "brand_name",
            "prod_mnf",
            "product_id", "product_name",
            "specialty"
        )
        .agg(
            sum("trx_unit").alias("total_trx_unit"),
            sum("trx_vol").alias("total_trx_vol"),
            sum("trx_dollar").alias("total_trx_dollar")
        )
        .orderBy(desc("total_trx_unit"))
    )

# Find out the Top 10 districts based on the Sales from last year
@dp.materialized_view(
    name="g_top_10_districts_by_sales",
    comment="Gold: Top 10 districts by total TRx units (last year)"
)
def g_top_10_districts_by_sales():

    sales = spark.table("sl_sales")
    addr  = spark.table("sl_prescriber_address")
    zt    = spark.table("sl_zip_to_terr")
    geo   = spark.table("sl_geo_hierarchy_resolved")
    prod  = spark.table("sl_product_master")

    # ---------------------------------------------------
    # 0) Product -> Prescriber
    # ---------------------------------------------------
    sales_prod = (
        sales
        .join(prod, sales.product_id == prod.prd_pack_id, "left")
    )
    # ---------------------------------------------------
    # 1) Prescriber -> ZIP
    # ---------------------------------------------------
    sales_zip = (
        sales_prod
        .join(addr, on="prescriber_id", how="left")
        .filter(col("zip_code").isNotNull())
    )

    # ---------------------------------------------------
    # 2) ZIP -> Territory
    # ---------------------------------------------------
    sales_terr = (
        sales_zip
        .join(zt, on="zip_code", how="left")
        .filter(col("territory_code").isNotNull())
    )

    # ---------------------------------------------------
    # 3) Territory -> District
    # ---------------------------------------------------
    sales_dist = (
        sales_terr
        .join(
            geo.select(
                "territory_code",
                "district_code",
                "district_name"
            ),
            on="territory_code",
            how="left"
        )
        .filter(col("district_code").isNotNull())
    )

    # ---------------------------------------------------
    # 4) Aggregate & Rank
    # ---------------------------------------------------
    return (
        sales_dist
        .groupBy("district_code", "district_name","prod_mnf")
        .agg(
            sum("t_rx_unit").alias("total_trx_unit"),
            sum("t_rx_vol").alias("total_trx_vol"),
            sum("t_rx_dollar").alias("total_trx_dollar")
        )
        .orderBy(desc("total_trx_unit"))
        .limit(10)
    )

#Top 5 best performing physicians in each City (in 2023) based on sales
@dp.materialized_view(
    name="g_top5_physicians_by_city",
    comment="Gold: Top 5 physicians in each city by total TRx dollar (last year)"
)
def g_top5_physicians_by_city():

    sales = spark.table("sl_sales")
    pres  = spark.table("sl_prescriber")
    addr  = spark.table("sl_prescriber_address")

    # ---------------------------------------------------
    # 1) Join sales -> prescriber -> address (to get city)
    # ---------------------------------------------------
    joined = (
        sales
        .join(pres, on="prescriber_id", how="left")
        .join(addr.select("prescriber_id", "city", "state", "zip_code"), on="prescriber_id", how="left")
        .filter(col("city").isNotNull())
    )

    # ---------------------------------------------------
    # 2) Aggregate at City + Physician (annual totals)
    # ---------------------------------------------------
    city_physician = (
        joined
        .groupBy(
            "city", "state",
            "prescriber_id",
            "first_name", "last_name", "preferred_name",
            "specialty"
        )
        .agg(
            sum("t_rx_dollar").alias("total_trx_dollar"),
            sum("t_rx_unit").alias("total_trx_unit")
        )
    )

    # ---------------------------------------------------
    # 3) Rank within each city by TRx Dollar, keep Top 5
    # ---------------------------------------------------
    w = Window.partitionBy("city", "state").orderBy(col("total_trx_dollar").desc())

    return (
        city_physician
        .withColumn("rank_in_city", row_number().over(w))
        .filter(col("rank_in_city") <= 5)
        .orderBy("city", "state", "rank_in_city")
    )


