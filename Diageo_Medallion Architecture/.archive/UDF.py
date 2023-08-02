# Databricks notebook source
# DBTITLE 1,Truncate and Load 
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def load_meta_table(csv_file_path, meta_table_name):
    df = spark.read.parquet(parquet_file_path)

    spark.sql(f"TRUNCATE TABLE {meta_table_name}")

    #Load the data from DataFrame into the meta table
    df.write.mode("append").saveAsTable(meta_table_name)



# COMMAND ----------

# DBTITLE 1,Delete and Load
def delete_and_load_meta_table(csv_file_path, meta_table_name):

    df = spark.read.parquet(parquet_file_path)

    spark.sql(f"DROP TABLE IF EXISTS {meta_table_name}")
    #Load the data from DataFrame into the meta table
    df.write.mode("append").saveAsTable(meta_table_name)


# COMMAND ----------

# DBTITLE 1,SCD 1
def scd_type_1(source_df, target_df, join_columns):
    # Update the target DataFrame with the contents of the source DataFrame based on join conditions
    target_df = target_df.alias("target")
    source_df = source_df.alias("source")
    target_df.join(source_df, on=join_columns, how="left") \
             .selectExpr("COALESCE(source.join_column1, target.join_column1) as join_column1",
                         "COALESCE(source.join_column2, target.join_column2) as join_column2",
                         # Include other columns with COALESCE function for updating
                         ) \
             .write.mode("overwrite").insertInto(target_df)


# COMMAND ----------

# DBTITLE 1,SCD 2
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, lit, current_date

def scd_type_2(source_df, target_df, join_columns, effective_date_col, method="start_date"):
    # Assign row numbers to the updates based on the effective_date_col in descending order
    updates = source_df.join(target_df, on=join_columns, how="inner")
    updates = updates.withColumn("row_number", row_number().over(Window.partitionBy(join_columns).orderBy(updates[effective_date_col].desc())))
    
    # Separate updates into new records and latest versions based on the selected method
    if method == "start_date":
        updates = updates.filter(updates["row_number"] == 1)
        updates = updates.withColumn("end_date", current_date())  # Update end_date to current date for the previous version
        new_records = source_df.join(target_df, on=join_columns, how="left_anti")
        new_records = new_records.withColumn(effective_date_col, current_date())  # Update effective_date_col to current date for new records
    elif method == "flag":
        updates = updates.withColumn("is_latest", lit(1)).where(col("row_number") == 1)
        updates = updates.withColumn("is_latest", col("is_latest").cast("int"))  # Cast is_latest to integer (0 or 1)

        # Set is_latest to 0 for updated records
        updated_records = updates.where(col("row_number") > 1).withColumn("is_latest", lit(0))
        updates = updates.filter(col("row_number") == 1).unionByName(updated_records)
        
        new_records = source_df.withColumn("is_latest", lit(1)).join(target_df, on=join_columns, how="left_anti")
    else:
        raise ValueError("Invalid method. Choose 'start_date' or 'flag'.")
    
    # Union the new_records and updates, then overwrite the target_df
    updates.unionByName(new_records).write.mode("overwrite").insertInto(target_df)


# COMMAND ----------

# DBTITLE 1,Null Handling
def handle_nulls(df, table_type):
    default_values = {
        StringType(): "UNK",
        DateType(): "2999-12-31",
        IntegerType(): -1 if table_type == "dim" else 0,
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
                F.lit(default_value)
            ).otherwise(df[col])
        )

    return df


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Lower the column name 
from pyspark.sql import functions as F
df.select([F.col(x).alias(x.lower()) for x in df.columns]).display()

# COMMAND ----------

# DBTITLE 1,Generating unique id
def generate_unique_id(df, *partition_cols):
    window_spec = Window.orderBy(*[df[col] for col in partition_cols])
    return df.withColumn("unique_id", row_number().over(window_spec) - 1)

# Call the UDF with the DataFrame and partition columns as arguments
df_with_unique_id = generate_unique_id(combined_df, "name")

df_with_unique_id.show()

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, expr, col
from pyspark.sql.functions import lower


def SCD1(source_df, key_column, table_name, join_keys, scd_columns):
    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()

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

            # After the SCD1 merge operation, overwrite the target table with the updated data from the Delta table
            delta_table.toDF().write.mode("overwrite").format("delta").saveAsTable(f'IN1510.{table_name}')

            print(f"Target table updated with the Delta table data for: {table_name}")

        load_end_time = datetime.now()
        triggered_by = spark.sql("SELECT current_user() as user").collect()[0].user
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = source_df_with_keys.count() - old_count

    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = spark.sql("SELECT current_user() as user").collect()[0].user
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

    # Capture the update events and create a DataFrame with the event information
    if spark.catalog.tableExists('IN1510.event_table'):
        target_df = delta_table.toDF()
        # Convert the 'companycode' column to lowercase in target_df and ensure the same data type
        for col_name in join_keys:
            target_df = target_df.withColumn(col_name, lower(col(col_name)))
            source_df_with_keys = source_df_with_keys.withColumn(col_name, source_df_with_keys[col_name].cast(target_df.schema[col_name].dataType))

        # Now perform the join
        join_condition = ' AND '.join(f'source_table.{key} = target_table.{key}' for key in join_keys)
        event_table_data = source_df_with_keys.alias('source_table').join(
            target_df.alias('target_table'),
            expr(join_condition),
            'inner'
                ).where(expr(' OR '.join(f'target_table.{col_name} != source_table.{col_name}' for col_name in scd_columns + join_keys)))

        # Select appropriate columns and rename them
        event_table_data = event_table_data.select(
            lit(table_name).alias('Target_Table_Name'),
            *(col(key).alias(key) for key in join_keys),
            lit(f'scd1_{scd_columns[0]}').alias('SCD_Column'),
            lit("update").alias('Event_Type'),
            col('source_table.*').alias('Record'),  # Using col to select all columns from source_table
            col('target_table.created_at').alias('Created_At'),
            current_timestamp().alias('Updated_At')
        )

        # Show the event table DataFrame
        event_table_data.show()

        # Save the event table DataFrame to the Delta event table
        event_table_data.write.mode("append").format("delta").saveAsTable("IN1510.event_table")

    return updated_control_table_df

# Example usage:
SCD1(Alc_Company_Master, "company_master_key", "Alc_Company_Master", ["companycode"], ["SalesOrgCode", "SalesOrgName"])


