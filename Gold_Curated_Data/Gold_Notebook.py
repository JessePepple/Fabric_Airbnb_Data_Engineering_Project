#!/usr/bin/env python
# coding: utf-8

# ## Gold_Notebook
# 
# null

# In[153]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


# # Configs

# In[154]:


source_schema = "silver"
source_table = "enr_airbnbhostings"
target_schema = "gold"
target_table = "dim_airbnb_hostings"

cdc_column = "last_updated"
primary_key = "host_id"
surrogate_key = "Dim_HostingsKey"
col_list = ["host_name", "host_since", "is_superhost", "response_rate", "hosts_firstname"]
backdated_refresh = ""

target_path = "abfss://3ee8a8e3-8aa7-4753-bfb1-117b1ac1fc7e@onelake.dfs.fabric.microsoft.com/a5ac1664-2d49-4884-9140-72ef20573330/Files/Dim_Hostings"


# In[155]:


if len(backdated_refresh) == 0:
    if spark.catalog.tableExists(f"gold_lakehouse.{target_schema}.{target_table}"):
        last_load = spark.sql(f"SELECT MAX({cdc_column}) FROM gold_lakehouse.{target_schema}.{target_table}").collect()[0][0]
    else:
        last_load = "1800-01-01 00:00:00"
else:
    last_load = backdated_refresh


# In[170]:


df_src = spark.sql(f"""
    SELECT DISTINCT * 
    FROM silver_lakehouse.{source_schema}.{source_table} 
    WHERE {cdc_column} > '{last_load}'
""")
display(df_src)


# # Load target data (existing SCD table)

# In[157]:


if spark.catalog.tableExists(f"gold_lakehouse.{target_schema}.{target_table}"):
    df_trg = spark.sql(f"SELECT * FROM gold_lakehouse.{target_schema}.{target_table}")
else:
    # Empty target template
    df_trg = spark.sql(f"""
        SELECT *,
               CAST(0 AS INT) AS {surrogate_key},
               CAST('1800-01-01 00:00:00' AS TIMESTAMP) AS scd_create_date,
               CAST('1800-01-01 00:00:00' AS TIMESTAMP) AS scd_update_date
        FROM silver_lakehouse.{source_schema}.{source_table}
        WHERE 1=0
    """)

display(df_trg)


# In[ ]:


df_src.createTempView("srchost")
df_trg.createTempView("trghost")


# In[158]:


df_join = spark.sql(f"""
    SELECT srchost.*, trghost.scd_create_date, trghost.scd_update_date, trghost.{surrogate_key}
    FROM srchost
    LEFT JOIN trghost
    ON srchost.{primary_key} = trghost.{primary_key}
""")
display(df_join)


# # Split New And Old Records

# In[159]:


df_old = df_join.filter(col(surrogate_key).isNotNull())
df_new = df_join.filter(col(surrogate_key).isNull())
display(df_old)
display(df_new)


# In[160]:


df_old_cur = df_old.withColumn("scd_create_date", current_timestamp())


# # Assigning Surrogate Keys To New Records

# In[161]:


if spark.catalog.tableExists(f"gold_lakehouse.{target_schema}.{target_table}"):
    max_surrogatekey = spark.sql(f"SELECT MAX({surrogate_key} FROM gold_lakehouse.{target_schema}.{target_table})").collect()[0][0]
else:
    max_surrogatekey = 0
    df_new_cur = df_new.withColumn(surrogate_key, monotonically_increasing_id() + 1 + lit(max_surrogatekey))\
    .withColumn("scd_create_date", current_timestamp())\
    .withColumn("scd_update_date", current_timestamp())

display(df_new_cur)


# ## Combining Old And New Records

# In[162]:


df_cur_union = df_new_cur.unionByName(df_old_cur)
display(df_cur_union)


# # SCD TYPE 2 Merge Preparation

# In[163]:


df_cur_union = (
    df_cur_union
    .drop("scd_update_date", "last_updated")
    .withColumnRenamed("scd_create_date", "StartDate")
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("isActive", lit(True))
)
display(df_cur_union)


# # Merge Into Delta

# In[164]:


if spark.catalog.tableExists(f"gold_lakehouse.{target_schema}.{target_table}"):
    dlt_obj = DeltaTable.forPath(spark, target_path)

    dlt_obj.alias("trg").merge(
        df_cur_union.alias("src"),
        f"src.{primary_key} = trg.{primary_key} AND trg.isActive = TRUE"
    )\
    .whenMatchedUpdate(
        condition=" OR ".join([f"src.{c} <> trg.{c}" for c in col_list]),
        set={
            "isActive": F.lit(False),
            "EndDate": F.current_timestamp()
        }
    )\
    .whenNotMatchedInsert(
        values={c: F.col(f"src.{c}") for c in ["StartDate", "EndDate", "isActive"] + [surrogate_key] + col_list}
    )\
    .execute()
else:
    df_cur_union.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", True)\
        .option("path", target_path)\
        .save()


# ## FACT LISTINGS CURATION

# In[165]:





# In[168]:


df_facts = spark.sql("""
SELECT
  b.Dim_BookingKey,
   p.Dim_PropertiesKey,
  h.Dim_HostingsKey
FROM gold_lakehouse.gold.dim_bookings b
LEFT JOIN gold_lakehouse.gold.dim_properties p
   ON b.listing_id = p.listing_id AND p.isActive = True
LEFT JOIN gold_lakehouse.gold.dim_hostings h
   ON p.host_id = h.host_id AND h.isActive = True
    
""")
display(df_facts)


# In[169]:


if spark.catalog.tableExists("gold_lakehouse.gold.fact_listings"):
    dlt_obj = DeltaTable.forPath(spark, "abfss://3ee8a8e3-8aa7-4753-bfb1-117b1ac1fc7e@onelake.dfs.fabric.microsoft.com/a5ac1664-2d49-4884-9140-72ef20573330/Files/Fact_Listings")
    dlt_obj.alias("trg").merge(df_facts.alias("src"), "trg.Dim_BookingKey >= src.Dim_BookingKey")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
    df_facts.write.format("delta").mode("append").option("path", "abfss://3ee8a8e3-8aa7-4753-bfb1-117b1ac1fc7e@onelake.dfs.fabric.microsoft.com/a5ac1664-2d49-4884-9140-72ef20573330/Files/Fact_Listings").save()

