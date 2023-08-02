# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType, DateType, LongType
from pyspark.sql.functions import to_date

# COMMAND ----------

CompanyMaster_Schema = StructType([
    StructField("company_master_key", IntegerType()),
    StructField("companycode", StringType()),
    StructField("companyname", StringType()),
    StructField("salesorgcode", StringType()),
    StructField("salesorgname", StringType()),
    StructField("reportinghierarchycode1", StringType()),
    StructField("reportinghierarchyname1", StringType()),
    StructField("reportinghierarchycode2", StringType()),
    StructField("reportinghierarchyname2", StringType()),
    StructField("reportinghierarchycode3", StringType()),
    StructField("reportinghierarchyname3", StringType()),
    StructField("created_at", DateType()),
    StructField("updated_at", DateType())
])

# COMMAND ----------

CustomerMaster_Schema = StructType([
    StructField("customer_master_key", IntegerType()),
    StructField("customercode", StringType()),
    StructField("customername", StringType()),
    StructField("address1", StringType()),
    StructField("address2", StringType()),
    StructField("address3", StringType()),
    StructField("countrycode", StringType()),
    StructField("countryname", StringType()),
    StructField("zonecode", StringType()),
    StructField("zonename", StringType()),
    StructField("statecode", StringType()),
    StructField("statename", StringType()),
    StructField("saledistrict", StringType()),
    StructField("postcode", IntegerType()),
    StructField("isactive", StringType()),
    StructField("rtm", StringType()),
    StructField("profitcentrecode", StringType()),
    StructField("profitcentrename", StringType()),
    StructField("costcentrecode", StringType()),
    StructField("costcentrename", StringType()),
    StructField("creditdays", IntegerType()),
    StructField("division", IntegerType()),
    StructField("markettype", StringType()),
    StructField("customerlevel1", StringType()),
    StructField("customerlevel2", StringType()),
    StructField("customerlevel3", StringType()),
    StructField("ref_cluster", StringType()),
    StructField("salesgroupcode", StringType()),
    StructField("salesgroup", StringType()),
    StructField("saleoffice", StringType()),
    StructField("saleofficecode", StringType()),
    StructField("localgroupcode", StringType()),
    StructField("localgroupname", StringType()),
    StructField("created_at", DateType()),
    StructField("updated_at", DateType())
])

# COMMAND ----------

ActivationMapping_Schema= StructType([
    StructField("activation_mapping_key", IntegerType()),
    StructField("promotion_code", StringType()),
    StructField("promotion_description", StringType()),
    StructField("promotion_type", StringType()),
    StructField("retailer_code", StringType()),
    StructField("effective_from", DateType()),
    StructField("effective_to", DateType()),
    StructField("mapping_level", StringType()),
    StructField("is_active", IntegerType())
])

# COMMAND ----------

ActivationMaster_Schema= StructType([
    StructField("activation_master_key", IntegerType()),
    StructField("promotioncode", StringType()),
    StructField("promotiondescription", StringType()),
    StructField("promotiontype", StringType()),
    StructField("productlevelcode", StringType()),
    StructField("effectivefrom", DateType()),
    StructField("effectiveto", DateType())
])

# COMMAND ----------

GeographyMaster_Schema= StructType([
    StructField("geography_master_key", IntegerType()),
    StructField("countrycode", StringType()),
    StructField("countryname", StringType()),
    StructField("zonecode", StringType()),
    StructField("zonename", StringType()),
    StructField("statecode", StringType()),
    StructField("statename", StringType())
])

# COMMAND ----------

OutletMaster_Schema= StructType([
    StructField("outlet_master_key", IntegerType()),
    StructField("outlet_key", IntegerType()),
    StructField("outlet_code", StringType()),
    StructField("outlet_name", StringType()),
    StructField("geo_key", IntegerType()),
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("geo_longitude", FloatType()),
    StructField("geo_latitude", FloatType()),
    StructField("outlet_status", StringType()),
    StructField("owner_code", StringType()),
    StructField("owner_name", StringType()),
    StructField("distrubutor_code", StringType()),
    StructField("market_type", StringType()),
    StructField("hub", StringType()),
    StructField("channel", StringType()),
    StructField("micro_channel", StringType()),
    StructField("group_outlet_code", StringType()),
    StructField("outlet_tier", StringType()),
    StructField("is_active", StringType()),
    StructField("business_unit", StringType()),
    StructField("sales_representative_code", StringType()),
    StructField("sales_territory", StringType()),
    StructField("contact_name", StringType()),
    StructField("contact_email", StringType()),
    StructField("license_type", StringType()),
    StructField("active_flag", IntegerType()),
    StructField("dw_created_date", TimestampType()),
    StructField("dw_updated_date", TimestampType()),
    StructField("dw_created_by", StringType()),
    StructField("dw_updated_by", StringType()),
    StructField("territory_code", StringType()),
    StructField("outlet_segment", StringType()),
    StructField("outlet_start_date", TimestampType()),
    StructField("outlet_end_date", TimestampType()),
    StructField("created_at", DateType()),
    StructField("updated_at", DateType())
])



# COMMAND ----------

PlantMaster_Schema= StructType([
    StructField("plant_master_key", IntegerType()),
    StructField("plantcode", StringType()),
    StructField("plantname", StringType()),
    StructField("companycode", IntegerType()),
    StructField("statecode", StringType()),
    StructField("statename", StringType()),
    StructField("created_at", DateType()),
    StructField("updated_at", DateType())
])

# COMMAND ----------

ProductMaster_Schema = StructType([
    StructField("product_master_key", IntegerType()),
    StructField("skucode", IntegerType()),
    StructField("itemname", StringType()),
    StructField("companycode", StringType()),
    StructField("industryname", StringType()),
    StructField("brandfamily", StringType()),
    StructField("reportingsegment", StringType()),
    StructField("brandpacksize", StringType()),
    StructField("packtypecode", StringType()),
    StructField("brandpacktype", StringType()),
    StructField("statecode", StringType()),
    StructField("state", StringType()),
    StructField("brandcode", StringType()),
    StructField("BrandName", StringType()),
    StructField("productcategory", StringType()),
    StructField("productsegment", StringType()),
    StructField("caseconfig", IntegerType()),
    StructField("division", StringType()),
    StructField("conversionfactor", IntegerType()),
    StructField("acquiredstartdate", TimestampType()),
    StructField("acquiredenddate", TimestampType()),
    StructField("disposedstartdate", TimestampType()),
    StructField("disposedenddate", TimestampType()),
    StructField("innovationflag", StringType()),
    StructField("flavour", StringType()),
    StructField("subflavour", StringType()),
    StructField("created_at", DateType()),
    StructField("updated_at", DateType())
])


# COMMAND ----------

CompetitorProductMaster_Schema = StructType([
    StructField("competitor_product_master_key", IntegerType()),
    StructField("skucode", IntegerType()),
    StructField("itemname", StringType()),
    StructField("companycode", StringType()),
    StructField("industryname", StringType()),
    StructField("brandfamily", StringType()),
    StructField("brandcode", StringType()),
    StructField("brandname", StringType()),
    StructField("reportingsegment", StringType()),
    StructField("brandpacksize", StringType()),
    StructField("packtypecode", StringType()),
    StructField("brandpacktype", StringType()),
    StructField("statecode", StringType()),
    StructField("state", StringType()),
    StructField("productcategory", StringType()),
    StructField("productsegment", StringType()),
    StructField("flavour", StringType()),
    StructField("subflavour", StringType()),
    StructField("division", StringType()),
    StructField("created_at", DateType()),
    StructField("updated_at", DateType())
])

# COMMAND ----------

Primary_Sales_Actuals_Schema = StructType([
    StructField("companycode", StringType()),
    StructField("SalesOrgCode", StringType()),
    StructField("distributionchannel", IntegerType()),
    StructField("division", IntegerType()),
    StructField("customercode", StringType()),
    StructField("customername", StringType()),
    StructField("countrycode", StringType()),
    StructField("zonecode", StringType()),
    StructField("statecode", StringType()),
    StructField("skucode", IntegerType()),
    StructField("plantcode", StringType()),
    StructField("transactiondate", DateType()),
    StructField("volumeactualcase", IntegerType())
])

# COMMAND ----------

Primary_Sales_Plan_AOP_Schema=StructType([
    StructField("statecode", StringType(), nullable = True),
    StructField("month", StringType(), nullable = True),
    StructField("skucode", IntegerType(), nullable = True),
    StructField("brandpacktype", StringType(), nullable = True),
    StructField("brandpacksize", StringType(), nullable = True),
    StructField("brandcode", StringType(), nullable = True),
    StructField("createdmonth", StringType(), nullable = True),
    StructField("planqty", FloatType(), nullable = True)

])

# COMMAND ----------

GLAccountMaster_Schema = StructType([
    StructField("gl_account_master_key", IntegerType()),
    StructField("generalledgercode", IntegerType()),
    StructField("generalledgername", StringType()),
    StructField("gltype", StringType()),
    StructField("currency", StringType()),
    StructField("reporting_hierarchy_name_l1", StringType()),
    StructField("reporting_hierarchy_name_l2", StringType()),
    StructField("reporting_hierarchy_name_l3", StringType()),
    StructField("reporting_hierarchy_name_l4", StringType())
])

# COMMAND ----------

Sales_Org_Cluster_Schema = StructType([
    StructField("sales_org_cluster_key", IntegerType()),
    StructField("cluster_name", StringType()),
    StructField("cluster_code", StringType()),
    StructField("cluster_id", IntegerType()),
    StructField("cluster_head", StringType()),
    StructField("cluster_sfa_code", StringType()),
    StructField("cluster_head_email", StringType()),
    StructField("cluster__head_mobile", LongType()),
    StructField("active_flag", IntegerType()),
    StructField("cluster_start_date", DateType()),
    StructField("cluster_end_date", DateType())
])

# COMMAND ----------

Sales_Org_TL_Schema = StructType([
    StructField("sales_org_tl_key", IntegerType()),
    StructField("tl_territory_name", StringType()),
    StructField("tl_territory_code", StringType()),
    StructField("tl_territory_id", IntegerType()),
    StructField("tl", StringType()),
    StructField("tl_sfa_code", StringType()),
    StructField("tl_code", IntegerType()),
    StructField("tl_email", StringType()),
    StructField("tl_mobile", LongType()),
    StructField("market_working_norms", IntegerType()),
    StructField("min_market_working_with_tse", IntegerType()),
    StructField("Active_flag", IntegerType()),
    StructField("tl_start_date", StringType()),
    StructField("tl_end_date", DateType()),
    StructField("cluster_code", StringType())
])

# COMMAND ----------

Sales_Org_TSE_Schema = StructType([
    StructField("sales_org_tse_key", IntegerType()),
    StructField("tse_territory_name", StringType()),
    StructField("tse_territory_code", StringType()),
    StructField("tse_territory_id", IntegerType()),
    StructField("tse", StringType()),
    StructField("tse_code", IntegerType()),
    StructField("tse_sfa_code", StringType()),
    StructField("tse_email", StringType()),
    StructField("tse_mobile", LongType()),
    StructField("tse_call_norm", IntegerType()),
    StructField("state", StringType()),
    StructField("state_code", StringType()),
    StructField("district", StringType()),
    StructField("district_code", StringType()),
    StructField("active_flag", IntegerType()),
    StructField("tse_start_date", DateType()),
    StructField("tse_end_date", DateType()),
    StructField("tl_code", StringType())
])

# COMMAND ----------

df_schema = [CompanyMaster_Schema, CustomerMaster_Schema, ActivationMapping_Schema, ActivationMaster_Schema, GeographyMaster_Schema, GLAccountMaster_Schema, OutletMaster_Schema, PlantMaster_Schema, ProductMaster_Schema, Sales_Org_Cluster_Schema, Sales_Org_TL_Schema, Sales_Org_TSE_Schema, CompetitorProductMaster_Schema, Primary_Sales_Actuals_Schema, Primary_Sales_Plan_AOP_Schema]

table_names = ["Alc_Company_Master", "Alc_Customer_Master", "Alc_Activation_Mapping", "Alc_Activation_Master", "Alc_Geography_Master", "Alc_GL_Account_Master", "Alc_Outlet_Master", "Alc_Plant_Master", "Alc_Product_Master", "Alc_Sales_Org_Cluster", "Alc_Sales_Org_TL", "Alc_Sales_Org_TSE", "Alc_Competitor_Product_Master", "Alc_Primary_Sales_Actuals", "Alc_Primary_Sales_Plan_AOP"]

def create_empty_delta_table(schema, table_name):
    # Create an empty DataFrame with the given schema
    df_final = spark.createDataFrame([], schema)

    # Write the empty DataFrame as a Delta table
    df_final.write.option("delta.enableChangeDataFeed", "true").mode("ignore").option("mergeSchema", "true").saveAsTable(f"IN1510.{table_name}")
    return df_final

# Perform the function for each schema and table name
for i in range(len(df_schema)):
    table_name = table_names[i]
    schema = df_schema[i]
    create_empty_delta_table(schema, table_name)
    print(f"Delta table '{table_name}' created.")

# COMMAND ----------

table_names = ["Alc_Company_Master", "Alc_Customer_Master", "Alc_Activation_Mapping", "Alc_Activation_Master", "Alc_Geography_Master", "Alc_GL_Account_Master", "Alc_Outlet_Master", "Alc_Plant_Master", "Alc_Product_Master", "Alc_Sales_Org_Cluster", "Alc_Sales_Org_TL", "Alc_Sales_Org_TSE", "Alc_Competitor_Product_Master", "Alc_Primary_Sales_Actuals", "Alc_Primary_Sales_Plan_AOP"]

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table in1510.Alc_Primary_Sales_Plan_AOP
