from dotenv import load_dotenv
load_dotenv()

import asyncio
from core.messaging.kafka.consumer import retrive_data_topic_loop

def save_instagram_data_in_datawarehouse(message):
  print('load_raw_data', message)

if __name__ == '__main__':
  asyncio.run(retrive_data_topic_loop(['batch_info_account_instagram'], save_instagram_data_in_datawarehouse))