## View Kafka Logs:

``` bash
docker exec -it kafka_broker bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic city_requests \
  --from-beginning
```