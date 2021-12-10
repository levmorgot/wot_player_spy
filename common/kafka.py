from contextlib import contextmanager
from kafka import KafkaProducer, KafkaConsumer


@contextmanager
def connect_kafka_producer(bootstrap_servers=("localhost:9092",)):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        yield _producer
    except Exception as ex:
        print("Exception while connecting Kafka producer")
        print(str(ex))
    finally:
        if _producer is not None:
            _producer.close()


@contextmanager
def connect_kafka_consumer(topic_name,
                           auto_offset_reset="latest",
                           bootstrap_servers=("localhost:9092",)
                           ):
    _consumer = None
    try:
        _consumer = KafkaConsumer(topic_name, auto_offset_reset=auto_offset_reset, bootstrap_servers=bootstrap_servers)
        yield _consumer
    except Exception as ex:
        print("Exception while connecting Kafka consumer")
        print(str(ex))
    finally:
        if _consumer is not None:
            _consumer.close()


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))