# pyspark functions
from pyspark.sql.functions import *
# URL processing
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
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-1282968b0e7f-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/mount_1282968b0e7f"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

display(dbutils.fs.ls("/mnt/mount_1282968b0e7f/topics/1282968b0e7f.pin/partition=0/"))

# For pin topic first (df_pin)
# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_pin = "/mnt/mount_1282968b0e7f/topics/1282968b0e7f.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_pin)
# Display Spark dataframe to check its content
display(df_pin)

# For geo topic second (df_geo)
# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_geo = "/mnt/mount_1282968b0e7f/topics/1282968b0e7f.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_geo)
# Display Spark dataframe to check its content
display(df_geo)

# For user topic last (df_user)
# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_user = "/mnt/mount_1282968b0e7f/topics/1282968b0e7f.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_user)
# Display Spark dataframe to check its content
display(df_user)


#Cleaning df_pin 
df_pin = df_pin.withColumn('description', when ((col('description') == 'No description available Story format') | (col('description') == ''), None).otherwise(col('description')))
df_pin = df_pin.withColumn('follower_count', when ((col('follower_count') == 'User Info Error') | (col('description') == ''), None).otherwise(col('follower_count')))
df_pin = df_pin.withColumn('image_src', when ((col('image_src') == 'Image src error.') | (col('description') == ''), None).otherwise(col('image_src')))
df_pin = df_pin.withColumn('poster_name', when ((col('poster_name') == 'User Info Error') | (col('poster_name') == ''), None).otherwise(col('poster_name')))
df_pin = df_pin.withColumn('tag_list', when ((col('tag_list') == 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e') | (col('tag_list') == ''), None).otherwise(col('tag_list')))
df_pin = df_pin.withColumn('title', when ((col('title') == 'No Title Data Available') | (col('title') == ''), None).otherwise(col('title')))

#transform follower_count column to ensure there are no letters, and convert to an integer
#df_pin.select("follower_count").show(30)

def standardize_follower_count(follower_count):
    if not follower_count:
        return follower_count
    elif follower_count[-1] == 'k':
        follower_count = follower_count[:-1] + '000'
    elif follower_count[-1] == 'M':
        follower_count = follower_count[:-1] + '000000'
    return follower_count
    
#print(df_pin.schema["follower_count"].dataType)

#apply function and convert to integer
from pyspark.sql.types import StringType, IntegerType

standardize_follower_count_UDF = udf(lambda x:standardize_follower_count(x)) 
df_pin = df_pin.withColumn('follower_count', standardize_follower_count_UDF(col('follower_count')))
df_pin = df_pin.withColumn('follower_count', col('follower_count').cast(IntegerType()))
#df_pin.display()

#confirm this has worked - should return IntegerType
#print(df_pin.schema["follower_count"].dataType)

#ensure relevant columns are integers
df_pin = df_pin.withColumn("downloaded", col("downloaded").cast("integer"))
df_pin = df_pin.withColumn("index", col("index").cast("integer"))

#ensure save_location only has file path
df_pin = df_pin.withColumn('save_location', regexp_replace(col('save_location'), 'Local save in ', ''))

#change index column to ind
df_pin = df_pin.withColumnRenamed("index","ind")

#reorder columns
df_pin = df_pin.select('ind',
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
