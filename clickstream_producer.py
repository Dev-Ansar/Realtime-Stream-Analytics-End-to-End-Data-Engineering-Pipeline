import time
from kafka import KafkaProducer
import json
from event_generator import EventGenerator  # Import the EventGenerator class

# Instantiate the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Update with your Kafka broker's address
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')  # Optionally serialize the key
)

# Instantiate the EventGenerator
event_gen = EventGenerator()

# Function to generate and send events to the relevant Kafka topics
def generate_and_send_event():
    # Generate a video playback event and send it to the 'videoplayback' topic
    video_event = event_gen.generate_video_playback_event()
    producer.send('videoplayback', key=video_event['userId'], value=video_event)
    print(f"Sent video playback event: {video_event}")

    # Generate a search event and send it to the 'searchevents' topic
    search_event = event_gen.generate_search_event()
    producer.send('searchevents', key=search_event['userId'], value=search_event)
    print(f"Sent search event: {search_event}")

    # Generate a like/dislike event and send it to the 'likedislikeevents' topic
    like_event = event_gen.generate_like_dislike_event()
    producer.send('likedislikeevents', key=like_event['userId'], value=like_event)
    print(f"Sent like/dislike event: {like_event}")

    # Generate a navigation event and send it to the 'navigationevents' topic
    navigation_event = event_gen.generate_navigation_event()
    producer.send('navigationevents', key=navigation_event['userId'], value=navigation_event)
    print(f"Sent navigation event: {navigation_event}")


# Main loop to generate and send events every two minutes
if __name__ == "__main__":
    while True:
        generate_and_send_event()
        time.sleep(1) # Wait for 100 seconds before generating and sending the next batch of events