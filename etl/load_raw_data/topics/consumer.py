from core.messaging.kafka.consumer import retrive_data_topic_loop
from .batch_info_account_instagram import BatchInfoAccountInstagramTopic
import asyncio

class Consumer:
  topics = [
    BatchInfoAccountInstagramTopic()
  ]

  async def consume(self):
    await asyncio.gather(*[retrive_data_topic_loop([topic.name], topic.execute) for topic in self.topics])
    print("All topics consumed")

consumer = Consumer()