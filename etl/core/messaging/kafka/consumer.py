import sys
from confluent_kafka import Consumer,KafkaError, KafkaException
import asyncio
import json

from ...env import CoreEnv


async def retrive_data_topic_loop(topics: list[str], callback):
  loop = asyncio.get_event_loop()
  
  conf_consumer = {
    'bootstrap.servers': CoreEnv().kafka_cluster_host,
    'group.id': 'load_raw_data',
    'auto.offset.reset': 'earliest', 
    'enable.auto.commit': 'false'
  }
  consumer = Consumer(conf_consumer)

  def _consume():
    try:
      consumer.subscribe(topics)
      while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: 
          continue

        if msg.error():
          if msg.error().code() == KafkaError._PARTITION_EOF:
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %(msg.topic(), msg.partition(), msg.offset()))
          else:
            raise KafkaException(msg.error())
        else:
          data = json.loads(msg.value().decode('utf-8'))
          callback(data)
          consumer.commit(asynchronous=False)  
    except Exception as e:
      print(f"Error to receive message kafka: {e}")
      raise e
    finally:
      consumer.close()

  await loop.run_in_executor(None, _consume)