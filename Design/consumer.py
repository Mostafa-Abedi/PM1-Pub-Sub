from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # For searching for the JSON file
import json
import os

# Automatically locate the JSON file for the service account key
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("No JSON service account key found in the current directory.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set project, topic, and subscription details
PROJECT_ID = "cloud-pubsub-449122"
SUBSCRIPTION_ID = "smartMeter-sub"

# Initialize Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
print(f"Listening for messages on {subscription_path}...\n")

# Callback function to handle incoming messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Deserialize the message
        message_data = json.loads(message.data.decode("utf-8"))
        print(f"Consumed message: {message_data}")

        # Acknowledge message processing completion
        message.ack()
    except Exception as e:
        print(f"Failed to process message. Error: {e}")

# Subscribe and listen to messages
if __name__ == "__main__":
    with subscriber:
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        try:
            streaming_pull_future.result()  # Block to process messages
        except KeyboardInterrupt:
            streaming_pull_future.cancel()  # Cancel subscription on exit
            print("Stopped listening for messages.")
