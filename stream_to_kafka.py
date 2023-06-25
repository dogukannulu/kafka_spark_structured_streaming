"""
Gets the random user API data and writes the data to a Kafka topic every 10 seconds
"""
import requests
import json
import time
from kafka import KafkaProducer


def create_response_dict(url: str="https://randomuser.me/api/?results=1") -> dict:
    """
    Creates the results JSON from the random user API call
    """
    response = requests.get(url)
    data = response.json()
    results = data["results"][0]

    return results


def create_final_json(results: dict) -> dict:
    """
    Creates the final JSON to be sent to Kafka topic only with necessary keys
    """
    kafka_data = {}

    kafka_data["full_name"] = f"{results['name']['title']}. {results['name']['first']} {results['name']['last']}"
    kafka_data["gender"] = results["gender"]
    kafka_data["location"] = f"{results['location']['street']['number']}, {results['location']['street']['name']}"
    kafka_data["city"] = results['location']['city']
    kafka_data["country"] = results['location']['country']
    kafka_data["postcode"] = int(results['location']['postcode'])
    kafka_data["latitude"] = float(results['location']['coordinates']['latitude'])
    kafka_data["longitude"] = float(results['location']['coordinates']['longitude'])
    kafka_data["email"] = results["email"]

    return kafka_data


def create_kafka_producer():
    """
    Creates the Kafka producer object
    """

    return KafkaProducer(bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'])

def start_streaming():
    """
    Writes the API data every 10 seconds to Kafka topic random_names
    """
    producer = create_kafka_producer()
    results = create_response_dict()
    kafka_data = create_final_json(results)    

    end_time = time.time() + 120 # the script will run for 2 minutes
    while True:
        if time.time() > end_time:
            break

        producer.send("random_names", json.dumps(kafka_data).encode('utf-8'))
        time.sleep(10)


if __name__ == "__main__":
    start_streaming()
