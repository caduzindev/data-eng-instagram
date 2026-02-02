from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
from topics.consumer import consumer

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == '__main__':
    print("Starting enriching consumer service...")
    asyncio.run(consumer.consume())
