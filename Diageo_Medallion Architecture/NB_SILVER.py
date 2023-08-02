# Databricks notebook source
# MAGIC %run /Users/karthiks1@systechusa.com/NB_BRONZE

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType, FloatType, LongType, DoubleType, TimestampType
from pyspark.sql.functions import col, to_date, lit, isnan, when, count, current_timestamp, expr, row_number, coalesce, udf,datediff
from delta.tables import DeltaTable
from datetime import datetime, timedelta, date 
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Define the paths to the Delta tables
alc_company_master_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.CompanyMaster_delta"
alc_customer_master_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.CustomerMaster_delta"
alc_activation_mapping_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.ActivationMapping_delta"
alc_activation_master_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.ActivationMaster_delta"
alc_geography_master_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.GeographyMaster_delta"
alc_outlet_master_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.OutletMaster_delta"
alc_plant_master_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.PlantMaster_delta"
alc_product_master_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.ProductMaster_delta"
alc_competitor_product_master_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.CompetitorProductMaster_delta"
alc_primary_sales_actuals_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.Primary_Sales_Actuals_delta"
alc_primary_sales_plan_aop_path = "dbfs:/FileStore/shared_uploads/IN1510/Alcobev.Primary_Sales_Plan_AOP_delta"
alc_sales_org_tl_path = "dbfs:/FileStore/shared_uploads/IN1510/Sales_Org_TL_delta"
alc_sales_org_tse_path = "dbfs:/FileStore/shared_uploads/IN1510/Sales_Org_TSE_delta"
alc_gl_account_master_path = "dbfs:/FileStore/shared_uploads/IN1510/GLAccountMaster_delta"
alc_sales_org_cluster_path = "dbfs:/FileStore/shared_uploads/IN1510/Sales_Org_Cluster_delta"

# COMMAND ----------

# DBTITLE 1,Read the Delta tables
Alc_Company_Master = spark.read.format("delta").load(alc_company_master_path)
Alc_Customer_Master = spark.read.format("delta").load(alc_customer_master_path)
Alc_Activation_Mapping = spark.read.format("delta").load(alc_activation_mapping_path)
Alc_Activation_Master = spark.read.format("delta").load(alc_activation_master_path)
Alc_Geography_Master = spark.read.format("delta").load(alc_geography_master_path)
Alc_Outlet_Master = spark.read.format("delta").load(alc_outlet_master_path)
Alc_Plant_Master = spark.read.format("delta").load(alc_plant_master_path)
Alc_Product_Master = spark.read.format("delta").load(alc_product_master_path)
Alc_Competitor_Product_Master = spark.read.format("delta").load(alc_competitor_product_master_path)
Alc_Primary_Sales_Actuals = spark.read.format("delta").load(alc_primary_sales_actuals_path)
Alc_Primary_Sales_Plan_AOP = spark.read.format("delta").load(alc_primary_sales_plan_aop_path)
Alc_Sales_Org_TL = spark.read.format("delta").load(alc_sales_org_tl_path)
Alc_Sales_Org_TSE = spark.read.format("delta").load(alc_sales_org_tse_path)
Alc_GL_Account_Master = spark.read.format("delta").load(alc_gl_account_master_path)
Alc_Sales_Org_Cluster = spark.read.format("delta").load(alc_sales_org_cluster_path)


# COMMAND ----------

# DBTITLE 1,Creating control table 
control_table_schema = StructType([
        StructField("Table_Name", StringType(), True),
        StructField("Load_Start_Datetime", TimestampType (), True),
        StructField("Load_End_Datetime", TimestampType (), True),
        StructField("Triggered_By", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("Next_Run", TimestampType (), True),
        StructField("Records_Processed", LongType(), True)
    ])

control_table_data = []
control_table_df = spark.createDataFrame(control_table_data, control_table_schema)

# Write the DataFrame to Delta Lake to create the control table
control_table_path = "dbfs:/FileStore/shared_uploads/IN1510/control_table"  
control_table_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(control_table_path)


# COMMAND ----------

# transformation for Alc_Customer_Master
Alc_Customer_Master = Alc_Customer_Master.withColumn("division", col("Division").cast(IntegerType())) \
                                         .withColumn("creditdays", col("CreditDays").cast(IntegerType())) \
                                         .withColumn("postcode", col("PostCode").cast(IntegerType()))

# transformation for Alc_Activation_Master
Alc_Activation_Master = Alc_Activation_Master.withColumn("effectivefrom", to_date(col("EffectiveFrom"), "dd-MM-yyyy")) \
                                             .withColumn("effectiveto", to_date(col("EffectiveTo"), "dd-MM-yyyy"))

# transformation for Alc_Activation_Mapping
Alc_Activation_Mapping = Alc_Activation_Mapping.withColumn("effectivefrom", to_date(col("EffectiveFrom"), "dd-MM-yyyy")) \
                                               .withColumn("effectiveto", to_date(col("EffectiveTo"), "dd-MM-yyyy")) \
                                               .withColumn("isactive", col("IsActive").cast(IntegerType()))

# transformation for Alc_Outlet_Master
Alc_Outlet_Master = Alc_Outlet_Master.withColumn("geo_key", col("geo_key").cast(IntegerType())) \
                                     .withColumn("active_flag", col("ACTIVE_FLAG").cast(IntegerType())) \
                                     .withColumn("outlet_key", col("OUTLET_KEY").cast(IntegerType()))

# transformation for Alc_Plant_Master
Alc_Plant_Master = Alc_Plant_Master.withColumn("companycode", col("companycode").cast(IntegerType()))

# transformation for Alc_Product_Master
Alc_Product_Master = Alc_Product_Master.withColumn("skucode", col("skucode").cast(IntegerType())) \
                                       .withColumn("caseconfig", col("caseconfig").cast(IntegerType())) \
                                       .withColumn("conversionfactor", col("conversionfactor").cast(IntegerType()))

# transformation for Alc_Competitor_Product_Master
Alc_Competitor_Product_Master = Alc_Competitor_Product_Master.withColumn("skucode", col("skucode").cast(IntegerType()))

# transformation for Alc_GL_Account_Master
Alc_GL_Account_Master = Alc_GL_Account_Master.withColumn("generalledgercode", col("generalledgercode").cast(IntegerType()))

# transformation for Alc_Sales_Org_Cluster
Alc_Sales_Org_Cluster = Alc_Sales_Org_Cluster.withColumn("cluster_start_date", to_date(col("cluster_start_date"), "dd-MM-yyyy")) \
                                             .withColumn("cluster_end_date", to_date(col("cluster_end_date"), "dd-MM-yyyy")) \
                                             .withColumn("cluster_id", col("cluster_id").cast(IntegerType()))

# transformation for Alc_Sales_Org_TL
Alc_Sales_Org_TL = Alc_Sales_Org_TL.withColumn("tl_start_date", to_date(col("tl_start_date"), "dd-MM-yyyy")) \
                                 .withColumn("tl_end_date", to_date(col("tl_end_date"), "dd-MM-yyyy")) \
                                 .withColumn("tl_territory_id", col("tl_territory_id").cast(IntegerType())) \
                                 .withColumn("min_market_working_with_tse", col("min_market_working_with_tse").cast(IntegerType())) \
                                 .withColumn("market_working_norms", col("market_working_norms").cast(IntegerType())) \
                                 .withColumn("active_flag", col("active_flag").cast(IntegerType())) \
                                 .withColumn("tl_code", col("tl_code").cast(IntegerType()))

# transformation for Alc_Sales_Org_TSE
Alc_Sales_Org_TSE = Alc_Sales_Org_TSE.withColumn("tse_start_date", to_date(col("tse_start_date"), "dd-MM-yyyy")) \
                                    .withColumn("tse_end_date", to_date(col("tse_end_date"), "dd-MM-yyyy")) \
                                    .withColumn("tse_territory_id", col("tse_territory_id").cast(IntegerType())) \
                                    .withColumn("tse_code", col("tse_code").cast(IntegerType())) \
                                    .withColumn("tse_call_norm", col("tse_call_norm").cast(IntegerType())) \
                                    .withColumn("active_flag", col("active_flag").cast(IntegerType()))

# transformation for Alc_Primary_Sales_Actuals
Alc_Primary_Sales_Actuals = Alc_Primary_Sales_Actuals.withColumn("distributionchannel", col("distributionchannel").cast(IntegerType())) \
                                                     .withColumn("division", col("division").cast(IntegerType())) \
                                                     .withColumn("skucode", col("skucode").cast(IntegerType())) \
                                                     .withColumn("volumeactualcase", col("volumeactualcase").cast(IntegerType())) \
                                                     .withColumn("transactiondate", to_date(col("transactiondate"), "dd-MM-yyyy"))

# transformation for Alc_Primary_Sales_Plan_AOP
Alc_Primary_Sales_Plan_AOP = Alc_Primary_Sales_Plan_AOP.withColumn("skucode", col("skucode").cast(IntegerType())) \
                                                       .withColumn("plan_qty", col("plan_qty").cast(FloatType()))

Alc_Primary_Sales_Plan_AOP = Alc_Primary_Sales_Plan_AOP.drop("plan_qty")

Alc_Primary_Sales_Plan_AOP = Alc_Primary_Sales_Plan_AOP.withColumn("month", F.to_date(F.concat(F.substring("month", 1, 3), F.lit("-"), F.substring("month", 5, 2), F.lit("-01")), "MMM-yy-dd"))
latest_month = Alc_Primary_Sales_Plan_AOP.select(F.max("month")).collect()[0][0]                                                


# COMMAND ----------

# DBTITLE 1,Function to handle Null Values
def handle_nulls(df, table_type):
    default_values = {
        StringType(): "UNK",
        DateType(): date(2999, 12, 31),  # Use Python datetime for DateType
        IntegerType(): -1 ,
        LongType(): 0,
        DoubleType(): 0
    }

    for col in df.columns:
        column_data_type = df.schema[col].dataType
        default_value = default_values.get(column_data_type, None)

        # Add handling for 'NULL' values
        df = df.withColumn(
            col,
            F.when(
                (df[col].isNull()) | (df[col] == "NULL"),
                default_value  # Use Python datetime for DateType
            ).otherwise(df[col])
        )

    return df


# COMMAND ----------

# DBTITLE 1,Handling null for all the Dataframes
Alc_Company_Master = handle_nulls(Alc_Company_Master, table_type = 'dim')
Alc_Customer_Master = handle_nulls(Alc_Customer_Master, table_type = 'dim')
Alc_Activation_Mapping = handle_nulls(Alc_Activation_Mapping, table_type = 'dim')
Alc_Activation_Master = handle_nulls(Alc_Activation_Master, table_type = 'dim')
Alc_Geography_Master = handle_nulls(Alc_Geography_Master, table_type = 'dim')
Alc_GL_Account_Master = handle_nulls(Alc_GL_Account_Master, table_type = 'dim')
Alc_Outlet_Master = handle_nulls(Alc_Outlet_Master, table_type = 'dim')
Alc_Plant_Master = handle_nulls(Alc_Plant_Master, table_type = 'dim')
Alc_Product_Master = handle_nulls(Alc_Product_Master, table_type = 'dim')
Alc_Sales_Org_Cluster = handle_nulls(Alc_Sales_Org_Cluster, table_type = 'dim')
Alc_Sales_Org_TL = handle_nulls(Alc_Sales_Org_TL, table_type = 'dim')
Alc_Sales_Org_TSE = handle_nulls(Alc_Sales_Org_TSE, table_type = 'dim')
Alc_Competitor_Product_Master = handle_nulls(Alc_Competitor_Product_Master, table_type = 'dim')
Alc_Primary_Sales_Actuals = handle_nulls(Alc_Primary_Sales_Actuals, table_type = 'fact')
Alc_Primary_Sales_Plan_AOP = handle_nulls(Alc_Primary_Sales_Plan_AOP, table_type = 'fact')

# COMMAND ----------

# DBTITLE 1,Adding Audit columns
def add_columns_with_defaults(input_df):
    # Get the current timestamp as Datetime
    current_timestamp = datetime.now()
    
    # Add the 'created_at' and 'created_by' columns with the current timestamp and constant value "IN1510"
    # 'created_at' will be of Datetime type
    input_df = input_df.withColumn("created_at", F.lit(current_timestamp)) \
                       .withColumn("created_by", F.lit("IN1510"))\
                           .withColumn("updated_at", F.lit(None).cast("timestamp"))


    return input_df


# COMMAND ----------

def add_columns_with_flag(input_df):

    input_df = input_df.withColumn("isactive", F.lit(1))


    return input_df
Alc_Outlet_Master = add_columns_with_flag(Alc_Outlet_Master)

# COMMAND ----------

# DBTITLE 1,Adding audit column for SCD Tables
Alc_Company_Master = add_columns_with_defaults(Alc_Company_Master)
Alc_Customer_Master = add_columns_with_defaults(Alc_Customer_Master)
Alc_Plant_Master = add_columns_with_defaults(Alc_Plant_Master)
Alc_Product_Master = add_columns_with_defaults(Alc_Product_Master)
Alc_Competitor_Product_Master = add_columns_with_defaults(Alc_Competitor_Product_Master)

# COMMAND ----------

# DBTITLE 1,Creating Final Table
def df_with_schema(df, table_name):
    df_schema = df.schema
    df_final = spark.createDataFrame(df.rdd, df_schema)
    
    # Check if the table exists in the catalog and drop it if it does
    if spark.catalog.tableExists(f"IN1510.{table_name}"):
        spark.sql(f"DROP TABLE IF EXISTS IN1510.{table_name}")
        print(f"Table IN1510.{table_name} dropped.")
    
    # Save the DataFrame as a Delta table
    df_final.write.option("delta.enableChangeDataFeed", "true").saveAsTable(f"IN1510.{table_name}")
    print(f"Data loaded to Delta table IN1510.{table_name}.")
    
    return df_final

# COMMAND ----------

# DBTITLE 1,Unique Key function
def skey_generation(df, key_column="key_column"):
    # Check if "key_column" is present in the DataFrame
    if key_column not in df.columns:
        # If "key_column" is not present, add a new column "key_column" with row numbers
        window_spec = Window.orderBy(lit(1)) 
        df = df.withColumn(key_column, row_number().over(window_spec))
    else:
        # Get the maximum value of the "key_column" in the DataFrame
        max_key = df.agg({key_column: "max"}).collect()[0][0]

        # Create a new DataFrame containing only the rows that have the "key_column" already assigned
        existing_rows_df = df.filter(col(key_column).isNotNull())

        # Assign unique sequential values to the "key_column" for new rows using row_number() window function
        new_rows_df = df.filter(col(key_column).isNull()) \
                        .withColumn(key_column, coalesce(row_number().over(Window.orderBy(lit(1))).cast("int") + max_key, lit(0)))

        # Combine the existing rows and new rows DataFrames and reorder columns
        df = existing_rows_df.union(new_rows_df).select([key_column] + [col_name for col_name in df.columns if col_name != key_column])

    return df


# COMMAND ----------

# DBTITLE 1,Adding Surrogate_key Columns to Dataframe
Alc_Company_Master = skey_generation(Alc_Company_Master, key_column = "company_master_key")
Alc_Customer_Master = skey_generation(Alc_Customer_Master, key_column = "customer_master_key")
Alc_Activation_Mapping = skey_generation(Alc_Activation_Mapping,key_column = "activation_mapping_key")
Alc_Activation_Master = skey_generation(Alc_Activation_Master,key_column = "activation_master_key")
Alc_Geography_Master = skey_generation(Alc_Geography_Master,key_column = "geography_master_key")
Alc_GL_Account_Master = skey_generation(Alc_GL_Account_Master,key_column = "gl_account_master_key")
Alc_Outlet_Master = skey_generation(Alc_Outlet_Master,key_column = "outlet_master_key")
Alc_Plant_Master = skey_generation(Alc_Plant_Master,key_column = "plant_master_key")
Alc_Product_Master = skey_generation(Alc_Product_Master,key_column = "product_master_key")
Alc_Sales_Org_Cluster = skey_generation(Alc_Sales_Org_Cluster,key_column = "sales_org_cluster_key")
Alc_Sales_Org_TL = skey_generation(Alc_Sales_Org_TL,key_column = "sales_org_tl_key")
Alc_Sales_Org_TSE = skey_generation(Alc_Sales_Org_TSE,key_column = "sales_org_tse_key")
Alc_Competitor_Product_Master = skey_generation(Alc_Competitor_Product_Master,key_column = "competitor_product_master_key")

# COMMAND ----------

Alc_Company_Master = skey_generation(Alc_Company_Master, key_column = "company_master_key")
Alc_Customer_Master = skey_generation(Alc_Customer_Master, key_column = "customer_master_key")
Alc_Activation_Mapping = skey_generation(Alc_Activation_Mapping,key_column = "activation_mapping_key")
Alc_Activation_Master = skey_generation(Alc_Activation_Master,key_column = "activation_master_key")
Alc_Geography_Master = skey_generation(Alc_Geography_Master,key_column = "geography_master_key")
Alc_GL_Account_Master = skey_generation(Alc_GL_Account_Master,key_column = "gl_account_master_key")
Alc_Outlet_Master = skey_generation(Alc_Outlet_Master,key_column = "outlet_master_key")
Alc_Plant_Master = skey_generation(Alc_Plant_Master,key_column = "plant_master_key")
Alc_Product_Master = skey_generation(Alc_Product_Master,key_column = "product_master_key")
Alc_Sales_Org_Cluster = skey_generation(Alc_Sales_Org_Cluster,key_column = "sales_org_cluster_key")
Alc_Sales_Org_TL = skey_generation(Alc_Sales_Org_TL,key_column = "sales_org_tl_key")
Alc_Sales_Org_TSE = skey_generation(Alc_Sales_Org_TSE,key_column = "sales_org_tse_key")
Alc_Competitor_Product_Master = skey_generation(Alc_Competitor_Product_Master,key_column = "competitor_product_master_key")

# COMMAND ----------

# DBTITLE 1,Update Control Table
def update_control_table(control_table_df, table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed):
    
    new_record = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    new_record_df = spark.createDataFrame(new_record, control_table_df.schema)
    control_table_df = control_table_df.union(new_record_df)

    return control_table_df


# COMMAND ----------

# DBTITLE 1,Truncate_and_load
def trunc_and_load(table_name, source_df):
    # Get the current timestamp as the load start time
    load_start_time = datetime.now()
    try:
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(table_name):
            # Truncate the target table using SQL DROP TABLE statement
            target_df = spark.read.table(f"IN1510.{table_name}")
            old_count = target_df.count()
            spark.sql(f"TRUNCATE TABLE IN1510.{table_name}")
            print(f"Table '{table_name}' truncated.")
            
        else:
            print(f"Table '{table_name}' does not exist.")
        # Write the DataFrame with the specified schema to the target table
        print(old_count)
        df_schema = source_df.schema
        df_with_schema = spark.createDataFrame(source_df.rdd, df_schema)
        df_with_schema.write.mode("overwrite").saveAsTable(f"IN1510.{table_name}")
        print(f"Data loaded to Delta table '{table_name}'.")
        # Get the current timestamp as the load end time
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = source_df.count() - old_count

        # Create a DataFrame for the updated control table
        control_table_data = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
        control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
        updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

        # Show the updated control table DataFrame
        updated_control_table_df.show()

        # Write the updated control table DataFrame to the control_table Delta table
        updated_control_table_df.write.mode("append").saveAsTable("IN1510.control_table")

        return updated_control_table_df

    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0

        # Create a DataFrame for the updated control table in case of failure
        control_table_data = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
        control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
        updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

        # Show the updated control table DataFrame
        updated_control_table_df.show()

        # Write the updated control table DataFrame to the control_table Delta table
        updated_control_table_df.write.mode("append").saveAsTable("IN1510.control_table")

        return updated_control_table_df


# COMMAND ----------

# DBTITLE 1,convert_to_delta
def convert_to_delta(df, table_name):
    # Save the DataFrame as a Delta table with the provided table name
    delta_table_path = f'dbfs:/user/hive/warehouse/IN1510/{table_name}'
    df.write.format("delta").mode("overwrite").save(delta_table_path)

    print(f"DataFrame has been converted and stored as a Delta table: {table_name}")


# COMMAND ----------

# DBTITLE 1,Creating Event table
def create_event_table(source_df, key_column, table_name, join_keys, scd_columns):


    # Read the target DataFrame from the table
    target_df = spark.read.table(f'IN1510.{table_name}')

    # Convert the 'companycode' column to lowercase in target_df and ensure the same data type
    for col_name in join_keys:
        target_df = target_df.withColumn(col_name, col(col_name).cast("string"))
        source_df = source_df.withColumn(col_name, source_df[col_name].cast("string"))

    # Now perform the join
    join_condition = ' AND '.join(f'source_table.{key} = target_table.{key}' for key in join_keys)
    event_table_data = source_df.alias('source_table').join(
        target_df.alias('target_table'),
        expr(join_condition),
        'inner'
    )

    # Calculate age in days using a UDF
    event_table_data = event_table_data.withColumn("Age", datediff(col('target_table.updated_at'), col('target_table.created_at'))) 

    # Check for changes in SCD columns
    scd_column_changed = expr(' OR '.join(f'target_table.{col_name} != source_table.{col_name}' for col_name in scd_columns))
    event_table_data = event_table_data.where(scd_column_changed)

    # Select appropriate columns and rename them
    event_table_data = event_table_data.select(
        col(f'target_table.{key_column}').alias('Surrogate_Key'),
        lit(table_name).alias('Table_Name'),
        lit(scd_columns[0]).alias('SCD_Column'),
        expr(f"concat_ws(', ', {', '.join(f'target_table.{col_name}' for col_name in scd_columns)})").alias('Records'),
        col('target_table.created_at').alias('Created_At'),
        col('target_table.updated_at').alias('Updated_At'),
        col('Age')
    )

    # Save the event table DataFrame to the Delta event table
    event_table_data.write \
        .option("mergeSchema", "true") \
        .mode("append") \
        .format("delta") \
        .saveAsTable("IN1510.event_table")


# COMMAND ----------

# DBTITLE 1,SCD 1
def SCD1(source_df, key_column, table_name, join_keys, scd_columns):
    load_start_time = datetime.now()
    old_count = 0

    try:
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(f'IN1510.{table_name}'):
            # Read the target DataFrame from the table (non-Delta format)
            target_df = spark.read.table(f'IN1510.{table_name}')

            # Convert the target DataFrame into a Delta table
            convert_to_delta(target_df, f'IN1510.{table_name}')

            # Get the path for the Delta table
            delta_table_path = f'dbfs:/user/hive/warehouse/IN1510/IN1510.{table_name}'
            delta_table = DeltaTable.forPath(spark, delta_table_path)
            old_count = target_df.count()

            source_df_with_keys = skey_generation(source_df, key_column)

            # Perform the SCD1 merge operation using DeltaTable.merge
            delta_table.alias('target_table') \
                .merge(
                    source_df_with_keys.alias('source_table'),
                    ' AND '.join(f'target_table.{key} = source_table.{key}' for key in join_keys)
                ) \
                .whenMatchedUpdate(
                    condition=expr(' OR '.join(f'target_table.{col_name} != source_table.{col_name}' for col_name in scd_columns + join_keys)),
                    set={
                        # Update SCD columns with the new values for the rows that have changed
                        **{col_name: f'source_table.{col_name}' for col_name in scd_columns},
                        # Update 'updated_at' column with the current timestamp
                        'updated_at': current_timestamp()
                    }
                ) \
                .whenNotMatchedInsertAll() \
                .execute()

            print(f"SCD1 merge operation completed for table: {table_name}")

            create_event_table(source_df, key_column, table_name, join_keys, scd_columns)
            

            # After the SCD1 merge operation, overwrite the target table with the updated data from the Delta table
            delta_table.toDF().write.mode("overwrite").format("delta").saveAsTable(f'IN1510.{table_name}')

            print(f"Target table updated with the Delta table data for: {table_name}")

        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = source_df_with_keys.count() - old_count

    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0

    control_table_data = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

    # Show the updated control table DataFrame
    updated_control_table_df.show()

    updated_control_table_df.write.mode("append").saveAsTable("IN1510.control_table")

    return updated_control_table_df



# COMMAND ----------

# DBTITLE 1,SCD 2
def SCD2(source_df, table_name, key_column, join_keys, scd_columns, scd_type='start_end_date'):
    load_start_time = datetime.now()
    old_count = 0
    
    try:
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(f'IN1510.{table_name}'):
            target_df = spark.read.table(f'IN1510.{table_name}')
            old_count = target_df.count()
            
            # Convert the target DataFrame into a Delta table
            delta_table_path = f'dbfs:/user/hive/warehouse/IN1510/IN1510.{table_name}'
            
            # Check if the Delta table already exists
            delta_table_exists = DeltaTable.isDeltaTable(spark, delta_table_path)

            if not delta_table_exists:
                # If the Delta table doesn't exist, convert the source DataFrame to a Delta table
                convert_to_delta(target_df, f'IN1510.{table_name}')
                
            source_df = skey_generation(source_df, key_column)
            
            if scd_type == 'start_end_date':
                # Add new columns 'start_date' and 'end_date' to the source DataFrame with the current timestamp
                source_df_with_dates = source_df.withColumn('start_date', lit(current_timestamp())) \
                                                .withColumn('end_date', lit('9999-12-31'))

                # Get the Delta table instance using an alias
                delta_table = DeltaTable.forName(spark, f'IN1510.{table_name}')

                # Perform the SCD2 merge operation using DeltaTable.merge
                existing_rows_df = delta_table.alias('target_table') \
                    .merge(
                        source_df_with_dates.alias('source_table'),
                        ' AND '.join(f'target_table.{key} = source_table.{key}' for key in join_keys)
                    ) \
                    .whenMatchedUpdate(
                        condition=' OR '.join(f'target_table.{col_name} != source_table.{col_name}' for col_name in scd_columns),
                        set={
                            'target_table.updated_at': expr('source_table.start_date')
                        }
                    ) \
                    .whenNotMatchedInsertAll() \
                    .execute()
            elif scd_type == 'flag':
                # Add a new column 'is_active' to the source DataFrame with a default value of 1
                source_df_with_flag = source_df.withColumn('is_active', lit(1))

                # Get the Delta table instance using an alias
                delta_table = DeltaTable.forName(spark, f'IN1510.{table_name}')

                # Perform the SCD2 merge operation using DeltaTable.merge
                existing_rows_df = delta_table.alias('target_table') \
                    .merge(
                        source_df_with_flag.alias('source_table'),
                        ' AND '.join(f'target_table.{key} = source_table.{key}' for key in join_keys)
                    ) \
                    .whenMatchedUpdate(
                        condition=' OR '.join(f'target_table.{col_name} != source_table.{col_name}' for col_name in scd_columns),
                        set={
                            'is_active': expr('0')
                        }
                    ) \
                    .whenNotMatchedInsertAll() \
                    .execute()

            print(f"SCD2 merge operation completed for table: {table_name}")

            # After the SCD2 merge operation, overwrite the target table with the updated data from the Delta table
            delta_table.toDF().write.mode("overwrite").format("delta").saveAsTable(f'IN1510.{table_name}')

            print(f"Target table updated with the Delta table data for: {table_name}")

        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = delta_table.toDF().count() - old_count

    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0

    control_table_data = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

    # Show the updated control table DataFrame
    updated_control_table_df.show()

    # Write the updated control table DataFrame to the control_table Delta table
    updated_control_table_df.write.mode("append").saveAsTable("IN1510.control_table")

    return updated_control_table_df



# COMMAND ----------

# DBTITLE 1,Alc_Primary_Sales_Plan_AOP
# Perform the joins and select columns
Alc_Primary_Sales_Plan_AOP = Alc_Primary_Sales_Plan_AOP.alias('a') \
.join(Alc_Geography_Master
.alias('b'), col('a.statecode') == col('b.statecode'), how="left") \
.join(Alc_Product_Master
.alias('c'), (col('a.skucode') == col('c.skucode')) & (col('a.brandcode') == col('c.brandcode')), how="left") \
.select(
Alc_Geography_Master
.geography_master_key.alias('geography_master_key'),
Alc_Product_Master
.product_master_key.alias('product_master_key'),
Alc_Primary_Sales_Plan_AOP['*'] 
)

# Add a new column "created_timestamp" with the current timestamp
Alc_Primary_Sales_Plan_AOP = Alc_Primary_Sales_Plan_AOP.withColumn("created_timestamp", current_timestamp())


# COMMAND ----------

# DBTITLE 1,Delete_and_load_table
def del_and_load_table(df, col_name, grain_columns, target_table):
    load_start_time = datetime.now()
    dfwithschema = spark.createDataFrame([], df.schema) 
    old_count = 0 
    
    try:
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(target_table):
            target_df = spark.read.table(f"IN1510.{target_table}")
            old_count = target_df.count()
            latest_month = df.selectExpr(f"MAX({col_name}) as max_month").collect()[0]["max_month"]
            condition = F.col(col_name) != latest_month
            for col in grain_columns:
                condition |= (F.col(col) != lit(df.select(col).first()[0]))
            filtered_df = df.filter(condition)
            filter_df = filtered_df.filter(F.date_format(col_name, "yyyy-MM-dd") != lit(latest_month))
            schema = filter_df.schema
            dfwithschema = spark.createDataFrame(filter_df.rdd, schema)
            
            # Filter the DataFrame to exclude records with the latest month
            dfwithschema.write.format("delta") \
                .mode("overwrite") \
                .option("delta.enableChangeDataFeed", "true") \
                .saveAsTable(target_table)
            
            print(f"Data loaded to Delta table '{target_table}'.")
            
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = dfwithschema.count() - old_count

    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0

    control_table_data = [(target_table, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

    # Show the updated control table DataFrame
    updated_control_table_df.show()

    # Write the updated control table DataFrame to the control_table Delta table
    updated_control_table_df.write.mode("append").saveAsTable("IN1510.control_table")

    return updated_control_table_df



# COMMAND ----------

# DBTITLE 1,Alc_Primary_Sales_Actuals
# Perform the joins and select columns
Alc_Primary_Sales_Actuals = Alc_Primary_Sales_Actuals.alias('a') \
.join(Alc_Geography_Master.alias('b'), (col('a.statecode') == col('b.statecode')) & (col('a.countrycode') == col('b.countrycode')), how="left") \
.join(Alc_Company_Master.alias('c'), col('a.companycode') == col('c.companycode'), how="left") \
.join(Alc_Customer_Master.alias('d'), col('a.customercode') == col('d.customercode'), how="left") \
.join(Alc_Product_Master.alias('e'), col('a.skucode') == col('e.skucode'), how="left") \
.join(Alc_Plant_Master.alias('f'), col('a.plantcode') == col('f.plantcode'), how="left") \
.select(
col('b.geography_master_key').alias('geography_master_key'),
col('c.company_master_key').alias('company_master_key'),
col('d.customer_master_key').alias('customer_master_key'),
col('e.product_master_key').alias('product_master_key'),
col('f.plant_master_key').alias('plant_master_key'),
Alc_Primary_Sales_Actuals['*']
)

# Add a new column "created_timestamp" with the current timestamp
Alc_Primary_Sales_Actuals = Alc_Primary_Sales_Actuals.withColumn("created_timestamp", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Incremental_load
def Incremental_load(source_df, target_table):
    load_start_time = datetime.now()
    df_final = spark.createDataFrame([], source_df.schema) 
    old_count = 0 
    
    try:
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(target_table):
            # Truncate the target table using SQL DROP TABLE statement
                target_table_df = spark.read.table(f"IN1510.{target_table}")
                max_date = target_table_df.agg({'transactiondate': 'max'}).collect()[0][0]

                # Filter the new data based on the maximum transaction date from the target table
                df_final = source_df.where(col("transactiondate") > max_date)

                # Save the filtered data to the target table
                df_final.write.mode('append').saveAsTable(f"IN1510.{target_table}")
                        
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = df_final.count() - old_count

    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0

    control_table_data = [(target_table, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

    # Show the updated control table DataFrame
    updated_control_table_df.show()

    # Write the updated control table DataFrame to the control_table Delta table
    updated_control_table_df.write.mode("append").saveAsTable("IN1510.control_table")

    return updated_control_table_df
