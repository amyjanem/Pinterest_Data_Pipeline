# Pinterest_Data_Pipeline

This project is currenlty in progress, however it involves building an end-to-end data processing pipeline inspired by Pinterest’s experiment processing pipeline.

The pipeline is developed using a Lambda architecture. The batch data is ingested using a FastAPI and Kafka and then stored in an AWS S3 bucket. The batch data is then read from the S3 bucket and processed using Apache Spark. The streaming data is read in real-time from Kafka using Spark Streaming and stored in a PostgreSQL database for analysis and long term storage.

The technologies that will be used are the following: Kafka, FastAPI, Spark, Spark Streaming, PostgresSQL and Airflow

**<ins>Pipeline Architecture</ins>**

<img width="801" alt="pipeline_arch" src="https://user-images.githubusercontent.com/105749987/223228145-795fc657-9ca8-47cb-9443-aad39d3c7112.png">


