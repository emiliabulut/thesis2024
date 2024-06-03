# All benefits by sex by month by year
# This notebook creates a table with the following columns: total_benefits, sex, month and year.

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

# Load the data from the table that was created as part of the data_cleanup notebook
all_data_df = spark.sql("SELECT * FROM silver_cleaned_2003_2023")
print(all_data_df.head())

# Drop the unnecessary columns
all_data_cleaned_df = all_data_df.drop('municipality_name', 'municipality_code')
print(all_data_cleaned_df.head())

# Only include the total compensation rows
filtered_df = all_data_cleaned_df[all_data_cleaned_df['compensation_type'] == 'Yhteensä']
print(filtered_df.head())

filtered_df = filtered_df.drop('compensation_type')

# Group the data by year and sex

filtered_df = filtered_df.withColumn("year", substring(filtered_df["year_month"], 0, 4))

all_data_grouped_df = filtered_df.drop('year_month')

all_data_grouped_df.head()

all_data_grouped_df = filtered_df.groupBy(['year', 'sex']).agg(_sum("receivers_amount").alias("receivers_amount_total"))
print(all_data_grouped_df.head())

# Add the necessary import statement for `DoubleType` from `pyspark.sql.types`
from pyspark.sql.types import DoubleType

all_data_grouped_df = all_data_grouped_df.withColumn("receivers_amount_total", col("receivers_amount_total").cast(DoubleType()))
all_data_grouped_df = all_data_grouped_df.withColumn("year", col("year").cast(IntegerType()))

import pyspark.sql.functions as F

all_data_grouped_df = all_data_grouped_df.withColumn("receivers_amount_total", F.expr("CASE WHEN year >= 2018 THEN receivers_amount_total/12 ELSE receivers_amount_total END"))

# Save DataFrame to storage account
all_data_grouped_df.write.mode("overwrite").format("delta").option("header", "true").save(gold_mount_name + "/all_benefits_by_sex_by_month_by_year")

# Create Delta Table based on delta files in storage account
query = f"""CREATE TABLE IF NOT EXISTS gold_all_benefits_by_sex_by_month_by_year (
    year INT,
    sex STRING,
    receivers_amount_total DOUBLE
) USING DELTA LOCATION '{gold_mount_name}/all_benefits_by_sex_by_month_by_year'"""
spark.sql(query)
