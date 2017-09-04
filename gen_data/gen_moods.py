"""
    Quick and dirty generator of random mood timeseries for 10 users.
    Event time and ingestion time are both expressed in milliseconds, 
     starting from 0 at the beginning of the simulation.
    All users emit a random mood at time zero, then review their mood 
    on average every 2s. 
"""

from __future__ import division
import faker
import numpy as np 
import json
from time import sleep
from kafka import KafkaProducer

target_topic="etj-moods-2"

# this seed must be identical to the seed for gen_events for the names to match
fake = faker.Faker()
fake.seed(1234)

state = np.random.RandomState(seed=2345)
kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def mood_duration_millis():
    """
    non negative random duration for the current mood of a user
    """
    return max(200, int(state.normal(2000, 1500)))


def random_mood():
    return state.choice(["happy", "neutral", "sad"])


def gen_mood_event(name, mood, event_time):

    return {
        "name": name, 
        "mood": mood,         
        "ingestion_time": current_time,
        "event_time": max(0, event_time)
        }


def emit_mood(name, late=True):
    if late:
        lateness = int(abs(state.normal(750, 500)))
    else:
        lateness = 0
 
    mood_event = gen_mood_event(name, 
        current_moods[name],
        current_time-lateness)

    mood_event_json = json.dumps(mood_event)

    print mood_event_json
    kafka_producer.send(target_topic, mood_event_json)


# a couple of nice globally shared variables, because I can :) 
names = [fake.first_name() for _ in range(10)]
current_moods = {name: random_mood() for name in names }
current_time = 0


if __name__ == "__main__":

    mood_durations = { name: mood_duration_millis() for name in names }

    # first initial mood
    for name, curr_mood in current_moods.items():
        emit_mood(name,  late=False)

    while True:
        wait_time = min(mood_durations.values())
        sleep(wait_time / 1000)
        current_time += wait_time
        mood_durations = { name: dur - wait_time for name, dur in mood_durations.items()}        

        users_to_update = [name for name, dur in mood_durations.items() if dur == 0]

        for name in users_to_update: 
            updated_mood = random_mood()
            mood_durations[name] = mood_duration_millis()
            if updated_mood != current_moods[name]:
                current_moods[name] = updated_mood
                emit_mood(name)
                







