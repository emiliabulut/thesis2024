# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType

storage_account_name = "thesis2024sa"
storage_account_access_key = "hLLEWD0OPoa+q2+opIQrSaapc7Su3gccKC4yvXlLzrzMchaWWbrif0Y2KUHz7tgkrrYR1IOeDRyj+ASt5FM57A=="

gold_container_name = "gold"
gold_mount_name = "/mnt/gold"

if not any(mount.mountPoint ==  gold_mount_name for mount in dbutils.fs.mounts()):
  # Mount Azure Blob Storage to DBFS
  dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(gold_container_name, storage_account_name),
    mount_point = gold_mount_name,
    extra_configs = {"fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name): storage_account_access_key}
  )

all_data_df = spark.sql("SELECT * FROM silver_cleaned_2003_2023")
print(all_data_df.head())

# Drop the columns municipality_code, municipality_name, sex from the all_data_df DataFrame
all_data_df = all_data_df.drop("municipality_code", "municipality_name", "sex")

# Filter rows where compensation_type is not equal to "Yhteensä"
filtered_df = all_data_df.filter(all_data_df.compensation_type != "Yhteensä")

# Show the result
filtered_df.show()

filtered_df = filtered_df.withColumn("year", substring(filtered_df["year_month"], 0, 4))

all_data_grouped_df = filtered_df.drop('year_month')

all_data_grouped_df.head()

all_data_grouped_df = filtered_df.groupBy(['year', 'compensation_type']).agg(_sum("receivers_amount").alias("receivers_amount_total"))
print(all_data_grouped_df.head())

# Add the necessary import statement for `DoubleType` from `pyspark.sql.types`
from pyspark.sql.types import DoubleType

all_data_grouped_df = all_data_grouped_df.withColumn("receivers_amount_total", col("receivers_amount_total").cast(DoubleType()))
all_data_grouped_df = all_data_grouped_df.withColumn("year", col("year").cast(IntegerType()))
print(all_data_grouped_df.head())

import pyspark.sql.functions as F

all_data_grouped_df = all_data_grouped_df.withColumn("receivers_amount_total", F.expr("CASE WHEN year >= 2018 THEN receivers_amount_total/12 ELSE receivers_amount_total END"))
print(all_data_grouped_df.head())

all_data_grouped_df.write.mode("overwrite").format("delta").option("header", "true").save(gold_mount_name + "/benefit_types_by_year")

query = f"""CREATE TABLE IF NOT EXISTS gold_benefit_types_by_year (
    year INT,
    compensation_type STRING,
    receivers_amount_total DOUBLE
) USING DELTA LOCATION '{gold_mount_name}/benefit_types_by_year'"""
spark.sql(query)
