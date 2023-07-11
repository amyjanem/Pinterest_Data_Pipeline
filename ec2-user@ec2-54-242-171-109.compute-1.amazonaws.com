from kafka import KafkaConsumer
from json import loads

#one consumer per topic

def my_kafka_consumer(my_topic_name, my_bootstrap_server):

    batch_consumer_ = KafkaConsumer(
        my_topic_name,
        bootstrap_servers=my_bootstrap_server,    
        value_deserializer=lambda message: loads(message),
        auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
    )

    # Loops through all messages in the consumer and prints them out individually
    for message in batch_consumer:
        print(message.value)
        print(message.topic)
        print(message.timestamp)


pintrest_bootstrap_server = ["b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098"]


"b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098",
"b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098"

my_kafka_consumer("1282968b0e7f.pin", pintrest_bootstrap_server)
my_kafka_consumer("1282968b0e7f.geo", pintrest_bootstrap_server)
my_kafka_consumer("1282968b0e7f.user", pintrest_bootstrap_server)

