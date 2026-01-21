from dotenv import load_dotenv
load_dotenv()

import asyncio
from .topics.consumer import consumer

if __name__ == '__main__':
  asyncio.run(consumer.consume())