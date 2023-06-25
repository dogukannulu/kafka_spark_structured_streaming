import requests
import json
import time
from kafka import KafkaProducer

def main():
    url = "https://randomuser.me/api/?results=1"
    producer = KafkaProducer(bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094']) 


    response = requests.get(url)
    data = response.json()
    results = data["results"][0]

    kafka_data = {}
    kafka_data["full_name"] = f"{results['name']['title']}. {results['name']['first']} {results['name']['last']}"
    kafka_data["gender"] = results["gender"]

    kafka_data["location"] = f"{results['location']['street']['number']}, {results['location']['street']['name']}"
    kafka_data["city"] = results['location']['city']
    kafka_data["country"] = results['location']['country']
    kafka_data["postcode"] = results['location']['postcode']
    kafka_data["latitude"] = float(results['location']['coordinates']['latitude'])
    kafka_data["longitude"] = float(results['location']['coordinates']['longitude'])
    kafka_data["email"] = results["email"]

    end_time = time.time() + 120 # the script will run for 2 minutes
    while True:
        if time.time() > end_time:
            break

        producer.send("random_names", json.dumps(kafka_data).encode('utf-8'))
        time.sleep(10)
