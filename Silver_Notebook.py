#!/usr/bin/env python
# coding: utf-8

# ## Silver_Notebook
# 
# null

# In[154]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[155]:


class SilverAirbnbTransformations:
    def __init__(self, df=None):
        self.df = df

    def read_data(self, file_name):
        self.df = spark.read.format("parquet").option("inferSchema", True).load(f"Files/{file_name}")
        return self.df

    def trans_int(self, col_type):
        for t in col_type:
            self.df = self.df.withColumn(t, col(t).cast(IntegerType()))
        
        return self.df

    def trans_date(self, date_col):
        for d in date_col:
            self.df = self.df.withColumn(d, col(d).cast(DateType()))
        
        return self.df

    def write_data(self, file_name):
        self.df.write.format("delta").mode("overwrite").option("overwriteSchema", True).option("path", f"abfss://3ee8a8e3-8aa7-4753-bfb1-117b1ac1fc7e@onelake.dfs.fabric.microsoft.com/de9d6b35-fafd-4f92-8479-290ce814ab41/Files/{file_name}").save()

    
    def splitdata(self, new_col, col1,):
        self.df = self.df.withColumn(new_col, split(col(col1)," ")[0])
        return self.df

    def cdccol(self, col_val):
        self.df = self.df.withColumn(col_val, current_timestamp())
        return self.df

    def dropdata(self, col_val):
        self.df = self.df.drop(col_val)
        return self.df


# In[156]:


Bookings = SilverAirbnbTransformations()


# In[157]:


df_bookings = Bookings.read_data("Airbnb_Bookings")


# In[158]:


display(df_bookings)


# In[159]:


df_bookings = Bookings.trans_int(["listing_id","nights_booked","booking_amount","cleaning_fee","service_fee"])
display(df_bookings)


# In[160]:


df_bookings = Bookings.trans_date(["booking_date", "created_at"])
display(df_bookings)


# In[161]:


df_bookings = df_bookings.withColumn(
    "booking_details",
    when(col("nights_booked") < 5, "Short Stay")
    .when((col("nights_booked") >= 5) & (col("nights_booked") <= 10), "Mid Stay")
    .otherwise("Long Stay")
)


# In[162]:


display(df_bookings)


# In[163]:


df_bookings = Bookings.cdccol("last_updated")
df_bookings = Bookings.dropdata("created_at")


# In[164]:


Bookings.write_data("Enr_AirbnbBookings")


# In[ ]:





# ## Hostings Transformations
# 

# In[165]:


Hostings = SilverAirbnbTransformations()


# In[166]:


df_hostings = Hostings.read_data("Airbnb_Hosts")
display(df_hostings)


# In[167]:


df_hostings = Hostings.trans_int(["host_id", "response_rate"])
display(df_hostings)


# In[168]:


df_hostings = Hostings.trans_date(["host_since", "created_at"])
display(df_hostings)


# In[169]:


df_hostings = Hostings.splitdata("hosts_firstname", "host_name")
display(df_hostings)


# In[170]:


df_hostings = df_hostings.select("host_id", "hosts_firstname","host_name", "host_since", "is_superhost", "response_rate", "created_at")\
.withColumnRenamed("host_name", "hosts_fullname")
display(df_hostings)


# In[171]:


df_hostings = Hostings.cdccol("last_updated")
df_hostings = Hostings.dropdata("created_at")


# In[172]:


Hostings.write_data("Enr_AirbnbHostings")


# ## Listings Transformations

# In[173]:


Listings = SilverAirbnbTransformations()


# In[174]:


df_listings = Listings.read_data("Airbnb_Listings")
display(df_listings)


# In[175]:


df_listings = Listings.trans_int(["listing_id", "host_id", "accommodates", "bedrooms","bathrooms", "price_per_night"])
df_listings = Listings.trans_date(["created_at"])
display(df_listings)


# In[176]:


df_listings = Listings.cdccol("last_updated")
df_listings = Listings.dropdata("created_at")


# In[177]:


display(df_listings)


# In[178]:


Listings.write_data("Enr_Airbnb_Listings")


# In[ ]:




