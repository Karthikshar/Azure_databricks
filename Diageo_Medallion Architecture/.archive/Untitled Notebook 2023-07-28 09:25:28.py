# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def scd1_udf(source_df, target_table, joining_cols, scd_cols):
    # Read the target table as a DataFrame
    target_df = spark.read.table(target_table)

    # Identify the columns that need to be inserted or updated
    insert_cols = [F.coalesce(source_df[col], F.lit(-1)).alias(col) for col in scd_cols]
    update_cols = [F.when(F.col(col) != source_df[col], source_df[col]).otherwise(F.col(col)).alias(col) for col in scd_cols]

    # Perform the SCD1 operations using left join and condition to identify updates
    updated_df = source_df.alias('s').join(target_df.alias('t'), on=joining_cols, how='left') \
        .where(F.col('t.' + joining_cols[0]).isNull()) \
        .select(*insert_cols)

    inserted_df = source_df.alias('s').join(target_df.alias('t'), on=joining_cols, how='left') \
        .where(F.col('t.' + joining_cols[0]).isNotNull()) \
        .select(*update_cols)

    # Insert new records into the target table
    if inserted_df.count() > 0:
        target_df = target_df.union(inserted_df)

    # Update existing records in the target table
    target_df = target_df.join(inserted_df.alias('i'), on=joining_cols, how='left_anti') \
        .join(updated_df.alias('u'), on=joining_cols, how='left_outer') \
        .select('t.*', *update_cols)

    # Save the updated target DataFrame back to the target table
    target_df.write.mode('overwrite').saveAsTable(target_table)



# Example Usage
source_df = spark.createDataFrame([
    (1, 'Tea', 101, 201, '10 bags', 5.99, 50, 10, 10, 0),
    (2, 'Coffee', 102, 201, '10 bags', 7.99, 40, 20, 15, 0)
], ['ProductID', 'ProductName', 'SupplierID', 'CategoryID', 'QuantityPerUnit', 'UnitPrice', 'UnitsInStock', 'UnitsOnOrder', 'ReorderLevel', 'Discontinued'])

target_table = 'Target_SCD1'
joining_cols = ['ProductID']
scd_cols = ['ProductID', 'ProductName', 'SupplierID', 'CategoryID', 'QuantityPerUnit', 'UnitPrice', 'UnitsInStock', 'UnitsOnOrder', 'ReorderLevel', 'Discontinued']

# Call the SCD1 UDF
scd1_udf(source_df, target_table, joining_cols, scd_cols)

# Show the updated target table
updated_target_df = spark.read.table(target_table)
updated_target_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession, functions as F

def scd1_udf(source_df, target_table, joining_cols, scd_cols):
    spark = SparkSession.builder.getOrCreate()

    # Read the target table as a DataFrame
    target_df = spark.read.table(f"IN1510.{target_table}")

    # Check if the target DataFrame has the 'updated_at' column, if not create it
    if 'updated_at' not in target_df.columns:
        target_df = target_df.withColumn('updated_at', F.lit(None))

    # Identify the columns that need to be inserted or updated
    insert_cols = [F.coalesce(source_df[col], F.lit(-1)).alias(col) for col in scd_cols]
    update_cols = [F.when(F.col('s.' + col) != source_df[col], source_df[col]).otherwise(F.col('t.' + col)).alias(col) for col in scd_cols]

    # Perform the SCD1 operations using left join and condition to identify updates
    # Perform the SCD1 operations using left join and condition to identify updates
    # Perform the SCD1 operations using left join and condition to identify updates
    updated_df = source_df.alias('s').join(target_df.alias('t'), on=joining_cols, how='left') \
        .select('t.*', *update_cols)\
        .withColumn('updated_at', F.when(F.col('t.' + joining_cols[0]).isNotNull(), 
                                        F.when(F.col('updated_at').isNull(), F.current_date()).otherwise(F.col('updated_at')))
                                    .otherwise(F.lit(None)))

    inserted_df = source_df.alias('s').join(target_df.alias('t'), on=joining_cols, how='left_anti') \
        .select(*insert_cols)

    # If there are no new records to insert, create an empty DataFrame with the same schema as target_df
    if inserted_df.count() == 0:
        schema = target_df.schema
        inserted_df = spark.createDataFrame([], schema)

    # Create the final DataFrame by performing union
    final_df = inserted_df.union(updated_df.select(target_df.columns))
    final_df.show()

    # Insert new records into the target table
    target_df = target_df.union(final_df)

    # Save the updated target DataFrame back to the target table
    target_df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f"IN1510.{target_table}")


# Example Usage
source_df = Alc_Competitor_Product_Master
target_table = 'Alc_Competitor_Product_Master'
joining_cols = ['skucode']
scd_cols = ["ReportingSegment", "ProductSegment"]

# Call the SCD1 UDF
scd1_udf(source_df, target_table, joining_cols, scd_cols)

# Show the updated target table
updated_target_df = spark.read.table(f"IN1510.{target_table}")
updated_target_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession, functions as F

def scd1_udf(source_df, target_table, joining_cols, scd_cols):
    spark = SparkSession.builder.getOrCreate()

    # Read the target table as a DataFrame
    target_df = spark.read.table(f"IN1510.{target_table}")

    # Check if the target DataFrame has the 'updated_at' column, if not create it
    if 'updated_at' not in target_df.columns:
        target_df = target_df.withColumn('updated_at', F.lit(None))

    # Identify the columns that need to be inserted or updated
    insert_cols = [F.coalesce(source_df[col], F.lit(-1)).alias(col) for col in scd_cols]
    update_cols = [F.when(F.col('s.' + col) != source_df[col], source_df[col]).otherwise(F.col('t.' + col)).alias(col) for col in scd_cols]

    # Perform the SCD1 operations using left join and condition to identify updates
    updated_df = source_df.alias('s').join(target_df.alias('t'), on=joining_cols, how='left') \
        .where(F.col('t.' + joining_cols[0]).isNotNull()) \
        .select('t.*', *update_cols)\
        .withColumn('updated_at', F.when(F.col('updated_at').isNull(), F.current_date()).otherwise(F.col('updated_at')))

    inserted_df = source_df.alias('s').join(target_df.alias('t'), on=joining_cols, how='left_anti') \
        .select(*insert_cols)

    # If there are no new records to insert, create an empty DataFrame with the same schema as target_df
    if inserted_df.count() == 0:
        schema = updated_df.schema
        inserted_df = spark.createDataFrame([], schema)

    final_df = inserted_df.union(updated_df)
    final_df.display()
    
    # Insert new records into the target table
    target_df = target_df.union(final_df)

    # Save the updated target DataFrame back to the target table
    target_df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f"IN1510.{target_table}")

# Example Usage
source_df = Alc_Competitor_Product_Master
target_table = 'Alc_Competitor_Product_Master'
joining_cols = ['skucode']
scd_cols = ["ReportingSegment", "ProductSegment"]

# Call the SCD1 UDF
scd1_udf(source_df, target_table, joining_cols, scd_cols)

# Show the updated target table
updated_target_df = spark.read.table(f"IN1510.{target_table}")
updated_target_df.show()

