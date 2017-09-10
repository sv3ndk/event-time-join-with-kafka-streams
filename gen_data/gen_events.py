"""
    Quick and dirty generator for a stream of events containing business strategy recommendation by a set of users.
    Make sure Kafka is started and the etj-events topic is created
      => this posts the generated events there
"""

from __future__ import division
import faker
import numpy as np 
import time
import json
from kafka import KafkaProducer

state = np.random.RandomState(seed=2345)
current_time = 0
num_users = 5
target_topic="etj-events-10"

# this seed must be identical to the seed for gen_moods for the names to match
fake = faker.Faker()
fake.seed(1234)

names = [fake.first_name() for _ in range(num_users)]
kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# setting a fake start "wallclock" time one hour ago
start_time = int(round(time.time() * 1000)) - 60*60*1000

def event_time_1970(event_time):
    """
    For readability, this example uses 0-based event times in milliseconds
    => timestamp values are between 0 and 10000 if we run it for 10 seconds. 
    
    But kafka needs real 1970-based timestamps, among other things to handle
    event retention in the topics.
    """
    return event_time + start_time

def thinking_time_until_recommendation():
    return int(state.exponential(200))

def gen_business_quote(name):

    lateness = int(abs(state.normal(250, 300)))

    return {
        "consultant": name,
        "ingestion_time": current_time,
        "recommendation": fake.bs(),
        "event_time": current_time - lateness
    }

def emit(event):
    event_json = json.dumps(event)
    print "posting to {}: {}".format(target_topic, event_json)
    kafka_producer.send(
        topic=target_topic, 
        value=event_json,
        key=event["consultant"].encode("UTF-8"),
        timestamp_ms=event_time_1970(event["event_time"]))

if __name__ == "__main__":

    thinking_times = {name : thinking_time_until_recommendation() for name in names}

    while True:
        wait_time = min(thinking_times.values())
        time.sleep(wait_time / 1000)
        current_time += wait_time
        thinking_times = { name: dur - wait_time for name, dur in thinking_times.items()}        

        speaking_users = [name for name, tt in thinking_times.items() if tt == 0]

        for name in speaking_users:
            event = gen_business_quote(name)
            emit(event)
            thinking_times[name] = thinking_time_until_recommendation()



    
