# Pinterest_Data_Pipeline

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, I created a similar system using the AWS Cloud. It involved building an end-to-end data processing pipeline inspired by Pinterest’s experiment processing pipeline.

The pipeline was developed using a Lambda architecture. The batch data was ingested using a FastAPI and Kafka and then stored in an AWS S3 bucket. The batch data was then read from the S3 bucket and processed using Apache Spark. The streaming data was read in real-time from Kafka using Spark Streaming and stored in a PostgreSQL database for analysis and long term storage.

The technologies that were be used are the following: Kafka, FastAPI, Spark, Spark Streaming, PostgresSQL and Airflow

**<ins>Pipeline Architecture</ins>**

<img width="801" alt="pipeline_arch" src="https://user-images.githubusercontent.com/105749987/223228145-795fc657-9ca8-47cb-9443-aad39d3c7112.png">


## **The Project Milestones are shown below:**

**<ins>Milestone 1: Set up the environment**
- Set up GitHub Repository and AWS Account

**<ins>Milestone 2: Get Started**
- Download Pintrest infrastructure (user_posting_emulation.py) which contains login credentials for a RDS database, which contains three tables with data resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest

**<ins>Milestone 3: Batch Processing - Configure the EC2 Kafka Client**
- Create .pem key file locally to allow one to connect to the EC2 instance
- Connect to the EC2 instance
- Set up Kafka on the EC2 instance
    - Install Kafka on client EC2 machine
    - Install IAM MSK authentication package on client EC2 machine
    - Configure Kafka Client to use AWS IAM authentication to the cluster by modifying the client.properties file
- Create Kafka topics: pin, geo & user

**<ins>Milestone 4: Batch Processing - Connect a MSK cluster to a S3 bucket**
- Create a custom plugin with MSK Connect
- Create a connector with MSK Connect

**<ins>Milestone 5: Batch Processing - Configuring an API in API Gateway**
- Build a Kafka REST Proxy integration method for API
- Set up the Kafka REST proxy on the EC2 client
- Send data to the API
    - This will in turn send data to the MSK cluster using the plugin-connector pair previously created
 
**<ins>Milestone 6: Batch Processing - Databricks**
- Set up Databricks account
- Mount S3 bucket to Databricks

**<ins>Milestone 7: Batch Processing - Spark on Databricks**
- Clean the dataframe containing information about Pintrest posts
- Clean the dataframe containing information about geolocation
- Clean the dataframe containing information about users
- Find most popular category in each country
- Find which was the most popular category of each year
- Find the user with the most followers in each country
- Find the most popular category for each age group
