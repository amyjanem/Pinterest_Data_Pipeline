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