import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
import asyncio
import json

from core.env import CoreEnv
from batch_info_account_instagram_success import BatchInfoAccountInstagramSuccessTopic

class Consumer:
    topics = [
        BatchInfoAccountInstagramSuccessTopic()
    ]

    async def consume(self):
        """Consome mensagens do Kafka de forma assíncrona."""
        loop = asyncio.get_event_loop()
        
        # Cria um consumer para cada tópico
        tasks = []
        for topic in self.topics:
            tasks.append(self._consume_topic(topic, loop))
        
        await asyncio.gather(*tasks)
        print("All topics consumed")
    
    async def _consume_topic(self, topic, loop):
        """Consome mensagens de um tópico específico."""
        conf_consumer = {
            'bootstrap.servers': CoreEnv().kafka_cluster_host,
            'group.id': 'enriching_consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false'
        }
        consumer = Consumer(conf_consumer)
        
        def _consume():
            try:
                consumer.subscribe([topic.name])
                while True:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            sys.stderr.write('%% %s [%d] reached end at offset %d\n' % 
                                           (msg.topic(), msg.partition(), msg.offset()))
                        else:
                            raise KafkaException(msg.error())
                    else:
                        data = json.loads(msg.value().decode('utf-8'))
                        # Executa o callback assíncrono
                        future = asyncio.run_coroutine_threadsafe(topic.execute(data), loop)
                        future.result()  # Aguarda a conclusão
                        consumer.commit(asynchronous=False)
            except Exception as e:
                print(f"Error to receive message kafka: {e}")
                raise e
            finally:
                consumer.close()
        
        await loop.run_in_executor(None, _consume)

consumer = Consumer()

