# messaging-simulation
messaging simulation using kafka + scylla

install requirement python package:
<pre> ``` 
  pip install -r requirements
``` </pre>

run docker compose :
<pre> ``` 
docker-compose -f docker-compose.kafka.yaml -f docker-compose.scylla.yaml up -d
``` </pre>

run the script:
<pre> ``` 
python producer.py
python consumer.py
``` </pre>
