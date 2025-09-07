# Create a test topic

docker exec kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1

# List topics to verify it was created

docker exec kafka kafka-topics --list --bootstrap-server localhost:29092

# Send a test message

echo "Hello Kafka" | docker exec -i kafka kafka-console-producer --topic test-topic --bootstrap-server localhost:29092

# Read the message back

docker exec kafka kafka-console-consumer --topic test-topic --bootstrap-server localhost:29092 --from-beginning --max-messages 1

# run local producers

python ecommerce_producers.py
