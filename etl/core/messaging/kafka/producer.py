from confluent_kafka import Producer
import socket
import json
import asyncio

from ...env import CoreEnv

conf_producer = {'bootstrap.servers': CoreEnv().kafka_cluster_host,'client.id': socket.gethostname()}
producer = Producer(conf_producer)

async def send_message_topic(topic: str, value: dict | list[dict]):
  loop = asyncio.get_event_loop()

  def _produce():
    try:
      producer.produce(
          topic=topic,
          value=json.dumps(value).encode('utf-8')
      )
      producer.poll(0)
    except Exception as e:
        print(f"Error to send message kafka: {e}")
        raise e

  await loop.run_in_executor(None, _produce)
