## This script will create random database operations and publish it to a Kafka Topic
import random
from io import BytesIO
from configparser import ConfigParser
from math import floor
from time import sleep
from fastavro import writer
from confluent_kafka import Producer

n_records = 0
n_initial_commands = 20
offset = 5
base_messages = 5

random.seed(42)

schema = {
    "namespace": "db_operation",
    "type": "record",
    "name": "Command",
    "fields": [
        {"name": "operation", "type": "string"},
        {
            "name": "payload",
            "type": {
                "type": "record",
                "name": "command_payload",
                "fields": [
                    {"name": "criteria", "type": ["string", "null"]},
                    {"name": "data", "type": "string"},
                ],
            },
        },
    ],
}


def init_producer() -> Producer:
    config_parser = ConfigParser()
    config_parser.read("getting_started.ini")
    config = dict(config_parser["default"])

    # Create Producer instance
    return Producer(config)


def serialize_message(message) -> bytes:
    out = BytesIO()
    writer(out, schema, [message])
    return out.getvalue()


def message_factory(operation):
    global n_records

    message = {"operation": operation, "payload": None}
    newId = f"AID{n_records}"
    if operation == "insert":
        message["payload"] = {
            "data": str(
                {
                    "_id": newId,
                    "probabilities": [],
                    "oid": "oid",
                    "firstmjd": 55555,
                    "lastmjd": 56666,
                    "detections": [],
                }
            )
        }

        n_records += 1

    else:
        message["payload"] = {
            "data": str(
                {"$set": {"probabilities": [{"thing": str(random.random() * 5000)}]}}
            ),
            "criteria": str({"_id": f"AID{floor(random.random() * n_records)}"}),
        }

    return message


def generate_initial_data():
    print(f"Producing initial {n_initial_commands} insertion commands")
    return [message_factory("insert") for _ in range(n_initial_commands)]


def generate_message():
    dice_roll = random.random()
    operation = "insert" if dice_roll < float(0.7) else "update"

    return message_factory(operation)


producer = init_producer()
initial_commands = generate_initial_data()

for i, command in enumerate(initial_commands):
    avro_command = serialize_message(command)
    producer.produce(
        "commands",
        value=avro_command,
        callback=lambda err, _: print(err) if err else None,
    )
    producer.poll(0)

producer.flush()

while True:
    try:
        n_new_messages = floor(random.random() * offset) + base_messages
        print(f"Generating {n_new_messages} new messages")
        for _ in range(n_new_messages):
            producer.produce("commands", value=serialize_message(generate_message()))

        producer.poll(10)
        producer.flush()

    except KeyboardInterrupt:
        break

    finally:
        sleep(5)

print("Gracefully ending...")
