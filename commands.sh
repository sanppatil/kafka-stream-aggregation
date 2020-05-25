
# Start producer to publish messages
 kafka-console-producer --broker-list localhost:9092 --topic streams-input-topic --property parse.key=true --property key.separator=:

# Start consumer to receive messages
 kafka-console-consumer --bootstrap-server localhost:9092 --topic streams-output-topic --property print.key=true
 