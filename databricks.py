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


#Task 1: Cleaning df_pin 
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

#Task 2: clean the df_geo
df_geo = df_geo.withColumn('coordinates', array('longitude', 'latitude'))
df_geo = df_geo.drop('longitude', 'latitude')
df_geo = df_geo.withColumn("timestamp",to_timestamp("timestamp"))

#print(df_geo.schema["timestamp"].dataType)

df_geo = df_geo.select('ind',
                       'country',
                       'coordinates',
                       'timestamp'
                       )
#df_geo.display()

#Task 3: Clean the user_df
df_user = df_user.withColumn(('user_name'), concat_ws(' ', 'first_name', 'last_name'))
df_user = df_user.drop('first_name', 'last_name')
df_user = df_user.withColumn('date_joined', to_timestamp('date_joined'))
#print(df_user.schema["date_joined"].dataType)

df_user = df_user.select('ind',
                         'user_name',
                         'age',
                         'date_joined'
                        )
#df_user.display()

#Create temp df's
df_geo.createOrReplaceTempView('countries')
df_pin.createOrReplaceTempView('posts')

#Task 4: Find the most popular category in each country
popular_category_by_country = spark.sql("""SELECT countries.country, posts.category, COUNT(*) AS category_count
                                  FROM countries JOIN posts ON countries.ind = posts.ind
                                  GROUP BY country, category
                                  ORDER BY category_count DESC""")

#popular_category_by_country.display()

#Task 5: Find how many posts each category had between the years or 2018 and 2022
popular_category_by_year = spark.sql("""SELECT year(countries.timestamp) AS post_year, category, COUNT(*) as category_count 
                                     FROM countries
                                     JOIN posts ON countries.ind = posts.ind
                                     WHERE (year(countries.timestamp) >= 2018 AND year(countries.timestamp) <= 2022)
                                     GROUP BY post_year, category
                                     ORDER BY post_year, category_count""")

#popular_category_by_year.display()

#Task 6: Find user with most followers in each country
df_user.createOrReplaceTempView('usernames')
#Step 1: for each country, find the user with the most followers
user_with_most_followers_per_country = spark.sql("""SELECT country, user_name AS poster_name, follower_count
                                                FROM countries 
                                                JOIN posts ON countries.ind = posts.ind
                                                JOIN usernames ON countries.ind = usernames.ind""")
                        
user_with_most_followers_per_country.display()

#Step 2: based on the above, find the country with the user with the most followers
country_with_user_with_most_followers = spark.sql("""SELECT country, MAX(follower_count) AS follower_count
                                                  FROM countries 
                                                  JOIN posts ON countries.ind = posts.ind
                                                  JOIN usernames ON countries.ind = usernames.ind
                                                  GROUP BY posts.follower_count, usernames.user_name, countries.country
                                                  ORDER BY follower_count DESC
                                                  LIMIT 1""")
                                                
#country_with_user_with_most_followers.display()

#Task 7: Most popular category based on age groups; 18-24, 25-35, 36-50, 50+
aged_based_popular_category = spark.sql("""WITH user_age_range AS (SELECT ind,
                                        CASE 
                                            WHEN age BETWEEN 18 AND 24 THEN '18-24' 
                                            WHEN age BETWEEN 25 AND 35 THEN '25-35' 
                                            WHEN age BETWEEN 36 AND 50 THEN '36-50' 
                                            ELSE '50+' 
                                        END AS age_group
                                        FROM usernames),
                                        
                                        joined_data AS (SELECT user_age_range.age_group, posts.category FROM user_age_range 
                                        JOIN posts ON user_age_range.ind = posts.ind), 
                                        
                                        category_count AS (SELECT age_group, category, COUNT(*) AS category_count 
                                        FROM joined_data GROUP BY age_group, category) 
                                        SELECT age_group, category, category_count 
                                        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY age_group ORDER BY category_count DESC) 
                                        AS rank FROM category_count) ranked_data WHERE rank = 1""")
                                        
                               
#aged_based_popular_category.display()
#figure out better way to do this

#Task 8: Median follower count for users in age groups 18024, 25-35, 36-50, 50+
newDF = df_user.join(df_pin, df_user.ind == df_pin.ind)
newDF = newDF.withColumn(
    "age_group",
    when((newDF["age"] >= 18) & (newDF["age"] <= 24), "18-24")
    .when((newDF["age"] >= 25) & (newDF["age"] <= 35), "25-35")
    .when((newDF["age"] >= 36) & (newDF["age"] <= 50), "36-50")
    .otherwise("50+")
)
task_8 = newDF.groupBy("age_group").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))

#task_8.show()

#Task 9: How many users have joined each year (2015 and 2020) -> return post_year and number_users_joined

quantity_users = spark.sql("""SELECT year(usernames.date_joined) AS post_year, COUNT(*) AS number_users_joined 
                           FROM usernames
                           WHERE year(usernames.date_joined) BETWEEN 2015 AND 2020
                           GROUP BY post_year""")

#quantity_users.display()

#Task 10: Find median follower count based on joining year (between 2015 and 2020) -> return post_year and median_follower_count

users_pin_df = df_user.join(df_pin, df_user.ind == df_pin.ind).filter(year(users_pin_df["date_joined"]) >= 2015).filter(year(users_pin_df["date_joined"]) <= 2020)

median_followers = users_pin_df.groupBy(year('date_joined')).agg(percentile_approx('follower_count', 0.5))

#median_followers.show()

#Task 11: Median follower count of users based on their joining year and age -> return age_group, post_year, median_follower_count

median_follower_age_datejoined = users_pin_df.groupBy("age", year("date_joined")).agg(percentile_approx("follower_count", 0.5)).sort("age")

#median_follower_age_datejoined.show()