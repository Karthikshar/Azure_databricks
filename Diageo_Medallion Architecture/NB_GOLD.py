# Databricks notebook source
# MAGIC %run /Users/karthiks1@systechusa.com/NB_SILVER

# COMMAND ----------

# DBTITLE 1,Creating final tables
df_with_schema(Alc_Company_Master,"Alc_Company_Master")
df_with_schema(Alc_Customer_Master,"Alc_Customer_Master")
df_with_schema(Alc_Activation_Mapping,"Alc_Activation_Mapping")
df_with_schema(Alc_Activation_Master,"Alc_Activation_Master")
df_with_schema(Alc_Geography_Master,"Alc_Geography_Master")
df_with_schema(Alc_GL_Account_Master,"Alc_GL_Account_Master")
df_with_schema(Alc_Outlet_Master,"Alc_Outlet_Master")
df_with_schema(Alc_Plant_Master,"Alc_Plant_Master")
df_with_schema(Alc_Product_Master,"Alc_Product_Master")
df_with_schema(Alc_Sales_Org_Cluster,"Alc_Sales_Org_Cluster")
df_with_schema(Alc_Sales_Org_TL,"Alc_Sales_Org_TL")
df_with_schema(Alc_Sales_Org_TSE,"Alc_Sales_Org_TSE")
df_with_schema(Alc_Competitor_Product_Master,"Alc_Competitor_Product_Master")
df_with_schema(Alc_Primary_Sales_Actuals,"Alc_Primary_Sales_Actuals")
df_with_schema(Alc_Primary_Sales_Plan_AOP,"Alc_Primary_Sales_Plan_AOP")

# COMMAND ----------

# DBTITLE 1,Performing Truncate_and_load
trunc_and_load("Alc_Activation_Mapping",Alc_Activation_Mapping)
trunc_and_load("Alc_Activation_Master",Alc_Activation_Master)
trunc_and_load("Alc_Geography_Master",Alc_Geography_Master)
trunc_and_load("Alc_GL_Account_Master",Alc_GL_Account_Master)
trunc_and_load("Alc_Sales_Org_Cluster",Alc_Sales_Org_Cluster)
trunc_and_load("Alc_Sales_Org_TL",Alc_Sales_Org_TL)
trunc_and_load("Alc_Sales_Org_TSE",Alc_Sales_Org_TSE)


# COMMAND ----------

# DBTITLE 1,Performing SCD1
SCD1(Alc_Company_Master,"company_master_key", "Alc_Company_Master", ["companycode"], ["SalesOrgCode", "SalesOrgName"])
SCD1(Alc_Plant_Master,"plant_master_key", "Alc_Plant_Master", ["plantcode"], ["PlantName"])
SCD1(Alc_Product_Master,"product_master_key", "Alc_Product_Master", ["skucode"], ["ReportingSegment"])
SCD1(Alc_Competitor_Product_Master,"competitor_product_master_key", "Alc_Competitor_Product_Master", ["skucode"], ["ReportingSegment", "ProductSegment"])

# COMMAND ----------

# DBTITLE 1,Performing SCD 2
SCD2(Alc_Customer_Master, "Alc_Customer_Master",'customer_master_key', ["customercode"], ["address1","address2","address3"], "start_end_date" )
SCD2(Alc_Outlet_Master, "Alc_Outlet_Master",'outlet_master_key', ["outlet_master_key","outlet_key","outlet_code"], ["outlet_status", "owner_code", "owner_name"], "flag")

# COMMAND ----------

# DBTITLE 1,Performing Delete and Load
del_and_load_table(Alc_Primary_Sales_Plan_AOP, 'month', ['skucode'], "Alc_Primary_Sales_Plan_AOP")

# COMMAND ----------

# DBTITLE 1,Performing Incremental Load
Incremental_load(Alc_Primary_Sales_Actuals, 'Alc_Primary_Sales_Actuals')
