# Databricks notebook source
import os

def dev_lab_setup(catalog_name, schema_name=None):
    if schema_name is None:
        schema_name=os.path.basename(os.getcwd())
        schema_name=schema_name.replace('-','_')
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")
    print(f"Using catalog: {catalog_name}")
    try:
        spark.sql(f"USE SCHEMA {schema_name}")
        print(f"Using schema: {schema_name}")
    except: 
        print(f"Schema {schema_name} does not exist. Creating it.")
        spark.sql(f"CREATE SCHEMA {schema_name}")
        spark.sql(f"USE SCHEMA {schema_name}")
        print(f"Schema {schema_name} created.\nUsing schema: {schema_name}")
    return None

# COMMAND ----------

def process_csv(databricks_share_name: str):
    # Read the CSV file from the volume with headers
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .load(f"/Volumes/{databricks_share_name}/v01/sf-listings/sf-airbnb.csv")

    # Check the schema and first few rows
    print("Schema:")
    df.printSchema()

    print("\nRow count:")
    print(df.count())

    print("\nSample data:")
    display(df.limit(5))

    # Write as a Delta table
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("sf_airbnb_listings")

    print("\nDelta table created successfully!")
    return df