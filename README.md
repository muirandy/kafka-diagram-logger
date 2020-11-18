# Kafka Diagram Logger

## Useful

Obtain the container Id for Kafka:
docker ps | grep kafka

List the topics on the running Kafka broker (works for TestContainers):
```
docker exec -it <> kafka-topics --list --bootstrap-server localhost:9092
```

```
docker exec -it 7ddc718d5d11 kafka-console-consumer --bootstrap-server localhost:9092 --property print.key=true --topic living-documentation --from-beginning
```