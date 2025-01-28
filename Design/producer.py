from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # For searching for the JSON file
import csv
import json
import os

# Automatically locate the JSON file for the service account key
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("No JSON service account key found in the current directory.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set project and topic details
PROJECT_ID = "cloud-pubsub-449122"
TOPIC_ID = "smartMeter"

# Initialize Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
print(f"Publishing messages to {topic_path}...\n")

# Read CSV file and publish messages
def publish_csv_to_topic(csv_file):
    with open(csv_file, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert row to JSON string
            message = json.dumps(row).encode("utf-8")

            try:
                # Publish serialized message
                future = publisher.publish(topic_path, message)
                future.result()  # Ensure message is sent successfully
                print(f"Published message: {row}")
            except Exception as e:
                print(f"Failed to publish message {row}. Error: {e}")

if __name__ == "__main__":
    CSV_FILE = "Labels.csv"  # Ensure Labels.csv exists in the same directory
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"{CSV_FILE} not found in the current directory.")
    publish_csv_to_topic(CSV_FILE)
