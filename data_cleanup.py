# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is meant to be run first

# COMMAND ----------

# MAGIC %md
# MAGIC Import the needed packages

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# COMMAND ----------

# Define the Azure Key Vault-backed secret scope and secret names
secret_scope = "keyvault-secret-scope"
client_id_secret_name = "databricks-sp-client-id"
client_secret_secret_name = "databricks-sp-client-secret"

# Retrieve the Service Principal credentials from Key Vault
client_id = dbutils.secrets.get(scope = secret_scope, key = client_id_secret_name)
client_secret = dbutils.secrets.get(scope = secret_scope, key = client_secret_secret_name)
tenant_id = "786aa7ee-ceaf-4d84-a9ea-db974f53a7fe"
storage_account_name = "thesis2024sa"

# Set necessary configurations for enabling Service Principal authentication 
spark.conf.set("spark.hadoop.fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storage_account_name), "OAuth")
spark.conf.set("spark.hadoop.fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storage_account_name), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storage_account_name), client_id)
spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storage_account_name), client_secret)
spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storage_account_name), "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Storage Account

# COMMAND ----------

bronze_container_name = "bronze"
bronze_mount_name = "/mnt/bronze"

# Make this cell idempotent by mounting only if a mount does not already exist
if not any(mount.mountPoint ==  bronze_mount_name for mount in dbutils.fs.mounts()):
  # Mount bronze Container to DBFS
  dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(bronze_container_name, storage_account_name),
    mount_point = bronze_mount_name,
  )

silver_container_name = "silver"
silver_mount_name = "/mnt/silver"

if not any(mount.mountPoint ==  silver_mount_name for mount in dbutils.fs.mounts()):
  # Mount silver Container to DBFS
  dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(silver_container_name, storage_account_name),
    mount_point = silver_mount_name,
  )

# COMMAND ----------

schema = StructType([
    StructField("aikatyyppi", StringType(), True),
    StructField("kuukausi_nro", IntegerType(), True),
    StructField("vuosikuukausi", StringType(), True),
    StructField("vuosi", StringType(), True),
    StructField("kunta_nro", IntegerType(), True),
    StructField("kunta_nimi", StringType(), True),
    StructField("ikaryhma", StringType(), True),
    StructField("sukupuoli", StringType(), True),
    StructField("etuus", StringType(), True),
    StructField("korvausperuste", StringType(), True),
    StructField("korvauslaji", StringType(), True),
    StructField("saaja_lkm", IntegerType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC Load all data into a single Dataframe

# COMMAND ----------

df_combined = spark.read.format("csv").schema(schema).option("header", "true").load(bronze_mount_name + "/*.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC Only include "total rows" for the columns that we will drop

# COMMAND ----------

# In the df_combined Dataframe, only keep the rows where the value of column 'korvausperuste' is 'Yhteensä' and the value of column 'korvauslaji' is 'Yhteensä'
df_filtered = df_combined[(df_combined['korvausperuste'] == 'Yhteensä') & (df_combined['korvauslaji'] == 'Yhteensä')]

# COMMAND ----------

# MAGIC %md
# MAGIC Drop unnecessary columns and rename columns

# COMMAND ----------

# # Remove the following columns: aikatyyppi, kuukausi_nro, vuosi and korvauslaji
df_filtered = df_filtered.drop('ikaryhma', 'aikatyyppi', 'kuukausi_nro', 'vuosi', 'korvausperuste', 'korvauslaji')

df_filtered = df_filtered.withColumnRenamed("vuosikuukausi", "year_month").withColumnRenamed("kunta_nro", "municipality_code").withColumnRenamed("kunta_nimi", "municipality_name").withColumnRenamed("sukupuoli", "sex").withColumnRenamed("etuus", "compensation_type").withColumnRenamed("saaja_lkm", "receivers_amount")

df_filtered.head()


# COMMAND ----------

from pyspark.sql.functions import when

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Kotoutumistuki", "Integration allowance").otherwise(df_filtered["compensation_type"]))

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Koulutuspäiväraha", "Education allowance").otherwise(df_filtered["compensation_type"]))

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Liikkuvuusavustus", "Commuting and relocation allowance").otherwise(df_filtered["compensation_type"]))

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Muutosturvaraha", "Transition security allowance").otherwise(df_filtered["compensation_type"]))

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Peruspäiväraha", "Basic unemployment allowance").otherwise(df_filtered["compensation_type"]))

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Työmarkkinatuki", "Labour market subsidy").otherwise(df_filtered["compensation_type"]))

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Työvoimapoliittinen koulutustuki", "Labour market training allowance").otherwise(df_filtered["compensation_type"]))

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Vuorottelukorvaus", "Job alternation compensation").otherwise(df_filtered["compensation_type"]))

df_filtered = df_filtered.withColumn("compensation_type", when(df_filtered["compensation_type"] == "Yhdistelmätuki", "Combination allowance").otherwise(df_filtered["compensation_type"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Delta Table

# COMMAND ----------

# Save the Dataframe to storage
df_filtered.write.mode("overwrite").format("delta").option("header", "true").save(silver_mount_name + "/2003_2023_delta")

# Create the Delta Table based on the files in storage
query = f"""CREATE TABLE IF NOT EXISTS silver_cleaned_2003_2023 (
    year_month STRING,
    municipality_code INT,
    municipality_name STRING, 
    sex STRING, 
    compensation_type STRING, 
    receivers_amount INT
) USING DELTA LOCATION '{silver_mount_name}/2003_2023_delta'"""
spark.sql(query)