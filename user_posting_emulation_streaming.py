import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            #print(pin_result)
            #print(geo_result)
            #print(user_result)

            # Sending post data stream to the API
            # invoke url for one record, if you want to put more records replace record with records
            invoke_url =  'https://rryghwc4f6.execute-api.us-east-1.amazonaws.com/test/streams/streaming-1282968b0e7f-pin/record'
                            
            #To send JSON messages you need to follow this structure
            payload = json.dumps({
                "stream-name": "YourStreamName",
                "Data": {
                        "index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"], "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"], "category": pin_result["category"]
                        },
                        "PartitionKey": "desired-name"
                        })

            headers = {'Content-Type': 'application/json'}
            response = requests.request("PUT", invoke_url, headers=headers, data=payload)
            print('\npin status code below:')
            print(response.status_code)
            print(response.text)


            # Sending geolocation data stream to the API
            invoke_url = "https://rryghwc4f6.execute-api.us-east-1.amazonaws.com/test/streams/streaming-1282968b0e7f-geo/record"

            payload = json.dumps({
                "StreamName": "YourStreamName",
                "Data": {
                        "ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], "country": geo_result["country"]
                        },
                        "PartitionKey": "desired-name"
                        }, default=str)

            headers = {'Content-Type': 'application/json'}
            response = requests.request("PUT", invoke_url, headers=headers, data=payload)
            print('\ngeo status code below:')
            print(response.status_code)
            print(response.text)
            
            
            # Sending user data stream to the API
            invoke_url = "https://rryghwc4f6.execute-api.us-east-1.amazonaws.com/test/streams/streaming-1282968b0e7f-user/record"

            payload = json.dumps({
                "StreamName": "YourStreamName",
                "Data": {
                        "ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": user_result["date_joined"]
                        },
                        "PartitionKey": "desired-name"
                        }, default=str)

            headers = {'Content-Type': 'application/json'}
            response = requests.request("PUT", invoke_url, headers=headers, data=payload)
            print('\nuser status code below:')
            print(response.status_code)
            print(response.text)



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    



