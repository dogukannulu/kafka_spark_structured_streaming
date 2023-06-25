# Information
![image](https://github.com/dogukannulu/kafka_spark_structured_streaming/assets/91257958/cce4c4fc-5280-45e1-8213-38345289670e)



This repo illustrates the Spark Structured Streaming

Gets random names from the API. Sends the name data to Kafka topics every 10 seconds using Airflow. Every message is read by Kafka consumer using Spark Structured Streaming and written to Cassandra table on a regular interval.

`stream_to_kafka_dag.py` -> The DAG script that writes the API data to a Kafka producer every 10 seconds.

`stream_to_kafka.py` -> The script that gets the data from API and sends it to Kafka topic

`spark_streaming.py` -> The script that consumes the data fromo Kafka topic with Spark Structured Streaming

`response.json` -> Sample response coming from the API


## Apache Airflow

Run the following command to clone the necessary repo on your local

```bash
git clone https://github.com/dogukannulu/docker-airflow.git
```
After cloning the repo, run the following command only once:

```bash
docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .
```

Then change the docker-compose-LocalExecutor.yml file with the one in this repo and add `requirements.txt` file in the folder. This will bind the Airflow container with Kafka and Spark container and necessary modules will automatically be installed:

```bash
docker-compose -f docker-compose-LocalExecutor.yml up -d
```

Now you have a running Airflow container and you can access the UI at `https://localhost:8080`

## Apache Kafka

`docker-compose.yml` will create a multinode Kafka cluster. We can define the replication factor as 3 since there are 3 nodes (kafka1, kafka2, kafka3). We can also see the Kafka UI on `localhost:8888`. 

We should only run:

```bash
docker-compose up -d
```

<img width="1424" alt="image" src="https://github.com/dogukannulu/kafka_spark_structured_streaming/assets/91257958/84037d58-cd3f-486f-afaa-a5a8a3ebad20">



After accessing to Kafka UI, we can create the topic `random_names`. Then, we can see the messages coming to Kafka topic:


<img width="434" alt="image" src="https://github.com/dogukannulu/kafka_spark_structured_streaming/assets/91257958/d63600dc-2cda-4ec7-b17f-9e15822eae62">


## Cassandra
`docker-compose.yml` will also create a Cassandra server. Every env variable is located in `docker-compose.yml`. I also defined them in the scripts.

By running the following command, we can access to Cassandra server:

```bash
docker exec -it cassandra /bin/bash
```

After accessing the bash, we can run the following command to access to cqlsh cli.

```bash
cqlsh -u cassandra -p cassandra
```

Then, we can run the following commands to create the keyspace `spark_streaming` and the table `random_names`:

```bash
CREATE KEYSPACE spark_streaming WITH replication = {'class':'SimpleStrategy','replication_factor':1};
```

```bash
CREATE TABLE spark_streaming.random_names(full_name text primary key, gender text, location text, city text, country text, postcode int, latitude float, longitude float, email text);
DESCRIBE spark_streaming.random_names;
```

![image](https://github.com/dogukannulu/kafka_spark_structured_streaming/assets/91257958/103cf8ab-a46d-4341-911d-8cac9f8b11f5)


## Running DAGs

We should move `stream_to_kafka.py` and `stream_to_kafka_dag.py` scripts under `dags` folder in `docker-airflow` repo. Then we can see that `random_people_names` appears in DAGS page.


When we turn the OFF button to ON, we can see that the data will be sent to Kafka topics every 10 seconds. We can check from Kafka UI as well.

<img width="1423" alt="image" src="https://github.com/dogukannulu/kafka_spark_structured_streaming/assets/91257958/4c85153f-d88a-41fa-a7bb-e8c1135852ce">


## Spark
First of all we should copy the local PySpark script into the container:

```bash
docker cp spark_streaming.py spark_master:/opt/bitnami/spark/
```

We should then access the Spark container and install necessary JAR files under jars directory.

```bash
docker exec -it spark_master /bin/bash
```

We should run the following commands to install the necessary JAR files under for Spark version 3.3.0:

```bash
cd jars
curl -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar
curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.3.0/spark-sql-kafka-0-10_2.13-3.3.0.jar
```

While the API data is sent to the Kafka topic `random_names` regularly, we can submit the PySpark application and write the topic data to Cassandra table:

```bash
cd ..
spark-submit --master local[2] --jars /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.3.0.jar,/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.3.0.jar spark_streaming.py
```

After running the commmand, we can see that the data is populated into Cassandra table

![image](https://github.com/dogukannulu/kafka_spark_structured_streaming/assets/91257958/36edc374-81d7-40d8-a017-80647ef5a903)

Enjoy :)


