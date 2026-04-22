from kafka import KafkaConsumer
from const import *
import sys

# Create consumer: Option 1 -- only consume new events
consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

# Create consumer: Option 2 -- consume old events (uncomment to test -- and comment Option 1 above)
#consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')

try:
  topic = sys.argv[1]
except:
  print ('Usage: python3 consumer <topic_name>')
  exit(1)
  
consumer.subscribe([topic])
for msg in consumer:
    print (msg.value)
