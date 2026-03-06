from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

catalog_name = "abccompany"
schema_name = "fixeddata"
volume_name = "abcpharmadata"

# spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
# spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

raw_base_path = f"/Volumes/{catalog_name}/{schema_name}"
raw_path = f"{raw_base_path}/{volume_name}"

# -----------------------------
# Pipeline for viewing the volumes
# -----------------------------
# def _list_volume_files(path: str):
#     # returns python list of file names
#     return [f.name for f in dbutils.fs.ls(path)]

# @dp.materialized_view(
#     name="bs__volume_file_list",
#     comment="Lists the raw files present in the input volume path"
# )
# def bs__volume_file_list():
#     files = _list_volume_files(raw_path)
#     # create a DataFrame with one row per file
#     return spark.createDataFrame([(raw_path, f) for f in files], ["volume_path", "file_name"])

######################################################################
#Base table creationg start from here:
# First we will create a base ingestion function so that we not need to copy and paste again and again

def _read_base_csv(file_name,sep="|"):
    df =  (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", sep)
                .csv(f"{raw_path}/{file_name}")
    )
    
    # ✅ Normalize column names (spaces → underscore, upper → lower optional)
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip().replace(" ", "_"))

    return df


# Now we will create the Base Tables for all the files:
@dp.materialized_view(name="bs_geo_hierarchy", comment="Base: Geo hierarchy mapping")
def bs_geo_hierarchy():
    return _read_base_csv("GeoHiery.txt")

@dp.materialized_view(name="bs_market_definition", comment="Base: Market description")
def bs_mkt_desc():
    return _read_base_csv("MarketDefinition.txt",'|')

@dp.materialized_view(name="bs_prescriber_address", comment="Base: Prescriber address")
def bs_person_address():
    return _read_base_csv("PrescriberAddress.txt",'|')

@dp.materialized_view(name="bs_prescriber", comment="Base: Prescriber profile")
def bs_person_profile():
    return _read_base_csv("Prescriber.txt",'|')

@dp.materialized_view(name="bs_product_master", comment="Base: Product master")
def bs_prod_master():
    return _read_base_csv("ProductMaster.txt",'|')

@dp.materialized_view(name="bs_sales", comment="Base: Sales (wide monthly columns)")
def bs_sales():
    return _read_base_csv("Sales.txt",'|')

@dp.materialized_view(name="bs_zip_to_terr", comment="Base: Zip to territory mapping")
def bs_zip_terr():
    return _read_base_csv("Zip_To_Terr.txt",'|')

# @dp.table(name="new_table")
# def new_table():
#     # return (
#     #     spark.read
#     #     .option("sep", "|")
#     #     .option("header", "true")
#     #     .option("inferSchema", "true")
#     #     .csv("/Volumes/abccompany/fixeddata/abcpharmadata/MarketDefinition.txt")
#     # )
#     df = spark.read \
#         .format("csv") \
#         .option("header","true") \
#         .option("sep" ,"|") \
#         .load("/Volumes/abccompany/fixeddata/abcpharmadata/MarketDefinition.txt")
#     return df

# @dp.table()
# def single_json():
#     df = spark.read \
#         .format("json") \
#         .load("/Volumes/abccompany/fixeddata/abcpharmadata/singleLine.json")
#     return df

# @dp.table()
# def multi_json():
#     df = spark.read \
#         .format("json") \
#         .option("multiline","true") \
#         .load("/Volumes/abccompany/fixeddata/abcpharmadata/multiLine.json")
#     return df

# @dp.table()
# def file_json():
#     df = spark.read \
#         .format("json") \
#         .option("multiline","true") \
#         .load("/Volumes/abccompany/fixeddata/abcpharmadata/file.json")
#     return df


