#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, DateType
import pandas as pd
from pandas import json_normalize
import pymongo
from pyspark.ml.feature import OneHotEncoder


# In[2]:


# creating Spark Session
spark = SparkSession.builder.appName("CustomerSegmentation").getOrCreate()


# In[3]:


# Mongo Connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
database = client["ecommerce_db"]
customers = database["customers"]


# In[4]:


json_data =  list(customers.find())


# In[5]:


json_data


# In[6]:


cust_df = json_normalize(json_data, sep='_')


# In[7]:


cust_df


# In[8]:


cust_df.to_csv("customers.csv", index=False)


# In[9]:


cust_sparkdf = spark.read.csv("customers.csv", header='True', inferSchema='True')


# In[10]:


# Print the DataFrame vertically
cust_sparkdf.describe().show(vertical=True)


# In[11]:


# Knowing Basic datatypes
cust_sparkdf.printSchema()


# In[12]:


cust_sparkdf.columns


# In[13]:


cust_sparkdf = cust_sparkdf.drop('_id','Customer ID','First Name','Last Name','Email','Phone Number','Registration Date')


# In[14]:


cust_sparkdf = cust_sparkdf.drop('Location_Street','Location_City','Location_State', 'Location_Postal Code','Location_Country')


# In[15]:


# Knowing Basic datatypes
cust_sparkdf.printSchema()


# In[16]:


cust_sparkdf.show()


# In[17]:


from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline


indexer = StringIndexer(inputCol="Gender", outputCol="GenderIndex")

# Create a OneHotEncoder to perform one-hot encoding
encoder = OneHotEncoder(inputCol="GenderIndex", outputCol="GenderVec")

# Build a pipeline to sequentially apply StringIndexer and OneHotEncoder
pipeline = Pipeline(stages=[indexer, encoder])

# Fit and transform the pipeline on the DataFrame
model = pipeline.fit(cust_sparkdf)
transformed_df = model.transform(cust_sparkdf)


# In[18]:


transformed_df.columns


# In[19]:


transformed_df=transformed_df.drop('Gender')


# In[33]:


transformed_df.show()


# In[22]:


from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# List of feature column names
feature_columns = ['Age','Purchase Frequency','Average Order Value','Recency of Purchase','Average Items per Order','Loyalty Program Membership','Online Shopping','Open Rate of Emails','Click-Through Rate','Total Spending','Repeat Purchases','Social Media Engagement','Customer Feedback','GenderIndex','GenderVec']

# Assemble features into a vector column
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_with_features = assembler.transform(transformed_df)

# Standardize features
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
scaler_model = scaler.fit(data_with_features)
data_scaled = scaler_model.transform(data_with_features)

# Train KMeans
k = 3  # Number of clusters
kmeans = KMeans().setK(k).setSeed(1)
model = kmeans.fit(data_scaled)

# Get cluster assignments and cluster centers
clustered_data = model.transform(data_scaled)
cluster_centers = model.clusterCenters()

# Show cluster assignments
clustered_data.select("features", "prediction").show()

# Show cluster centers
for center in cluster_centers:
    print("Cluster Center:", center)


# In[ ]:


# age > 15 : 

