# Databricks notebook source
# DBTITLE 1,Import packages
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1, Function to remove spaces and convert to lowercase for column names
def format_column_name(column_name):
    return column_name.strip().replace(" ", "_").lower()

# COMMAND ----------

# DBTITLE 1,UDF to connect with SSMS 
def read_tables(username,password,tablename):
    url = "jdbc:sqlserver://systechtraining.database.windows.net:1433;databaseName=trainer"
    df = (spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", tablename)
    .option("user", username)
    .option("password", password)
    .load()
    )

    return df

# COMMAND ----------

list_tables = ['Alcobev.CompanyMaster'
,'Alcobev.CustomerMaster'
,'Alcobev.ActivationMapping'
,'Alcobev.ActivationMaster'
,'Alcobev.GeographyMaster'
,'Alcobev.OutletMaster'
,'Alcobev.PlantMaster'
,'Alcobev.ProductMaster'
,'Alcobev.CompetitorProductMaster'
,'Alcobev.Primary_Sales_Actuals'
,'Alcobev.Primary_Sales_Plan_AOP']

# COMMAND ----------

# DBTITLE 1,Loading SSMS tables to Delta table 
for i in list_tables:
    dbutils.fs.rm(f'dbfs:/FileStore/shared_uploads/IN1510/{i}', True)
    df = read_tables("brandmuscle_trainees","$yst3ch@!23",i)

    # Convert column names to lowercase
    lower_cols = df.select([F.col(x).alias(format_column_name(x)) for x in df.columns])

    # Save the DataFrame as a Delta table with Column Mapping enabled
    delta_path = f'dbfs:/FileStore/shared_uploads/IN1510/{i}_delta'
    lower_cols.write.format("delta").mode("overwrite").save(delta_path)


# COMMAND ----------

# DBTITLE 1,Loading CSV files into delta table 
# Define the paths to the CSV files
sales_org_tl_path = "dbfs:/FileStore/shared_uploads/karthiks1@systechusa.com/Sales_Org_TL.csv"
sales_org_tse_path = "dbfs:/FileStore/shared_uploads/karthiks1@systechusa.com/Sales_Org_TSE.csv"
gl_account_master_path = "dbfs:/FileStore/shared_uploads/karthiks1@systechusa.com/GLAccountMaster.csv"
sales_org_cluster_path = "dbfs:/FileStore/shared_uploads/karthiks1@systechusa.com/Sales_Org_Cluster.csv"

# Read each CSV file and create DataFrames
Sales_Org_TL = spark.read.csv(sales_org_tl_path, header=True)
Sales_Org_TSE = spark.read.csv(sales_org_tse_path, header=True)
GLAccountMaster = spark.read.csv(gl_account_master_path, header=True)
Sales_Org_Cluster = spark.read.csv(sales_org_cluster_path, header=True)

# Applying column rename function for each dataframes
Sales_Org_TL = Sales_Org_TL.select([col(c).alias(format_column_name(c)) for c in Sales_Org_TL.columns])
Sales_Org_TSE = Sales_Org_TSE.select([col(c).alias(format_column_name(c)) for c in Sales_Org_TSE.columns])
GLAccountMaster = GLAccountMaster.select([col(c).alias(format_column_name(c)) for c in GLAccountMaster.columns])
Sales_Org_Cluster = Sales_Org_Cluster.select([col(c).alias(format_column_name(c)) for c in Sales_Org_Cluster.columns])


# Convert each DataFrame into Delta tables
Sales_Org_TL.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/IN1510/Sales_Org_TL_delta")
Sales_Org_TSE.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/IN1510/Sales_Org_TSE_delta")
GLAccountMaster.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/IN1510/GLAccountMaster_delta")
Sales_Org_Cluster.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/IN1510/Sales_Org_Cluster_delta")

