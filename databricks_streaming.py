#Read data from Kinesis streams into Databricks
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")


#Reading the pin streaming data
df_pin_kinesis = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-1282968b0e7f-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_pin_kinesis = df_pin_kinesis.selectExpr("CAST(data as STRING)") 
df_pin_kinesis.display()


# Defining the schema and types
df_pin_kinesis = df_pin_kinesis.withColumn('data', df_pin_kinesis['data'].cast(StringType()))
schema = StructType([StructField("index",IntegerType(),True),
                     StructField("unique_id",StringType(),True),
                     StructField("title",StringType(),True),
                     StructField("description",StringType(),True),
                     StructField("poster_name",StringType(),True),
                     StructField("follower_count",StringType(),True),
                     StructField("tag_list",StringType(),True),
                     StructField("is_image_or_video",StringType(),True),
                     StructField("image_src",StringType(),True),
                     StructField("downloaded",IntegerType(),True),
                     StructField("save_location",StringType(),True),
                     StructField("category",StringType(),True)
  ])

# transforming the data
json_pin = df_pin_kinesis.select(from_json("data", schema).alias("json"))
df_posts = (json_pin.select(col("json.index").alias("ind"),
                          col("json.unique_id").alias("unique_id"),
                          col("json.title").alias("title"),
                          col("json.description").alias("description"),
                          col("json.poster_name").alias("poster_name"),
                          col("json.follower_count").alias("follower_count"),
                          col("json.tag_list").alias("tag_list"),
                          col("json.is_image_or_video").alias("is_image_or_video"),
                          col("json.image_src").alias("image_src"),
                          col("json.downloaded").alias("downloaded"),
                          col("json.save_location").alias("save_location"),
                          col("json.category").alias("category")
                          ))


#Transform Kinesis streams in Databricks the same way as Batch Data
#Cleaning df_pin_kinesis 

df_posts = df_posts.withColumn('description', when ((col('description') == 'No description available Story format') | (col('description') == ''), None).otherwise(col('description')))
df_posts = df_posts.withColumn('follower_count', when ((col('follower_count') == 'User Info Error') | (col('description') == ''), None).otherwise(col('follower_count')))
df_posts = df_posts.withColumn('image_src', when ((col('image_src') == 'Image src error.') | (col('description') == ''), None).otherwise(col('image_src')))
df_posts = df_posts.withColumn('poster_name', when ((col('poster_name') == 'User Info Error') | (col('poster_name') == ''), None).otherwise(col('poster_name')))
df_posts = df_posts.withColumn('tag_list', when ((col('tag_list') == 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e') | (col('tag_list') == ''), None).otherwise(col('tag_list')))
df_posts = df_posts.withColumn('title', when ((col('title') == 'No Title Data Available') | (col('title') == ''), None).otherwise(col('title')))
#df_posts.display()

def standardize_follower_count(follower_count):
    """Converts data in follower_count column to IntegerType and removes any String letters"""
    if not follower_count:
        return follower_count
    elif follower_count[-1] == 'k':
        follower_count = follower_count[:-1] + '000'
    elif follower_count[-1] == 'M':
        follower_count = follower_count[:-1] + '000000'
    return follower_count
    
print(df_posts.schema["follower_count"].dataType)

#apply function and convert to integer
from pyspark.sql.types import StringType, IntegerType

standardize_follower_count_UDF = udf(lambda x:standardize_follower_count(x)) 
df_posts = df_posts.withColumn('follower_count', standardize_follower_count_UDF(col('follower_count')))
df_posts = df_posts.withColumn('follower_count', col('follower_count').cast(IntegerType()))
df_posts.display()

#confirm this has worked - should return IntegerType
print(df_posts.schema["follower_count"].dataType)

#ensure relevant columns are integers
df_posts = df_posts.withColumn("downloaded", col("downloaded").cast("integer"))
df_posts = df_posts.withColumn("ind", col("ind").cast("integer"))

#ensure save_location only has file path
df_posts = df_posts.withColumn('save_location', regexp_replace(col('save_location'), 'Local save in ', ''))

#reorder columns
df_posts = df_posts.select('ind',
                       'unique_id',
                       'title',
                       'description',
                       'follower_count',
                       'poster_name',
                       'tag_list',
                       'is_image_or_video',
                       'image_src',
                       'save_location',
                       'category'
                       )

#df_posts.display()

#Write streaming data to delta table in Databricks
df_posts.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .option("mergeSchema", "true") \
  .table("1282968b0e7f_pin_table")


#Reading the geo streaming data
#will be json object
df_geo_kinesis = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-1282968b0e7f-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_geo_kinesis = df_geo_kinesis.selectExpr("CAST(data as STRING)") 
#df_geo_kinesis.display()

#convert json object to dataframe
# Defining the scheme and types
df_geo_kinesis = df_geo_kinesis.withColumn("data", df_geo_kinesis["data"].cast(StringType()))
schema = StructType([StructField("ind",IntegerType(),True),
                     StructField("timestamp",TimestampType(),True),
                     StructField("latitude",FloatType(),True),
                     StructField("longitude",FloatType(),True),
                     StructField("country",StringType(),True)
  ])

# transforming the data
json_geo = df_geo_kinesis.select(from_json("data", schema).alias("json"))
countries = (json_geo.select(col("json.ind").alias("ind"),
                          col("json.timestamp").alias("timestamp"),
                          col("json.latitude").alias("latitude"),
                          col("json.longitude").alias("longitude"),
                          col("json.country").alias("country")
                          ))

#Clean the geo dataframe
countries = countries.withColumn("coordinates", array("longitude", "latitude"))
countries = countries.drop("longitude", "latitude")
countries = countries.withColumn("timestamp",to_timestamp("timestamp"))

print(countries.schema["timestamp"].dataType)

countries = countries.select("ind",
                       "country",
                       "coordinates",
                       "timestamp"
                       )
#countries.display()

#Write streaming data to delta table in Databricks
countries.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("1282968b0e7f_geo_table")


#Reading the user streaming data
df_user_kinesis = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-1282968b0e7f-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_user_kinesis = df_user_kinesis.selectExpr("CAST(data as STRING)") 
#df_user_kinesis.display()

# Transforming user data
df_df_user_kinesis = df_user_kinesis.withColumn('data', df_user_kinesis['data'].cast(StringType()))
schema = StructType([StructField("ind",IntegerType(),True),
                     StructField("first_name",StringType(),True),
                     StructField("last_name",StringType(),True),
                     StructField("age",IntegerType(),True),
                     StructField("date_joined",TimestampType(),True)
                    ])

json_user = df_user_kinesis.select(from_json("data", schema).alias("json"))
users = (json_user.select(col("json.ind").alias("ind"),
                          col("json.first_name").alias("first_name"),
                          col("json.last_name").alias("last_name"),
                          col("json.age").alias("age"),
                          col("json.date_joined").alias("date_joined")
                          )) 
#users.display()

#Clean the users dataframe
users = users.withColumn(('user_name'), concat_ws(' ', 'first_name', 'last_name'))
users = users.drop('first_name', 'last_name')
users = users.withColumn('date_joined', to_timestamp('date_joined'))
#print(users.schema["date_joined"].dataType)

users = users.select('ind',
                     'user_name',
                     'age',
                     'date_joined'
                    )
#users.display()

#Write streaming data to delta table in Databricks
users.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("1282968b0e7f_user_table")