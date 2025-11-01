set -euo pipefail

TOPIC="${TOPIC:-creditcard-transactions}"

docker compose pull
docker compose up -d

# Wait for the CLI to exist inside the container
echo "Waiting for Kafka CLI in container 'broker'..."
until docker exec broker sh -lc 'test -x /opt/kafka/bin/kafka-topics.sh'; do
  sleep 1
done

# Wait for the broker to accept commands on the INTERNAL listener (broker:29092)
echo "Waiting for Kafka broker to accept connections..."
until docker exec broker sh -lc '/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --list >/dev/null 2>&1'; do
  sleep 2
done

# Create the topic if missing
if ! docker exec broker sh -lc "/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --list | grep -Fx \"$TOPIC\" >/dev/null"; then
  docker exec broker sh -lc "/opt/kafka/bin/kafka-topics.sh --create --topic \"$TOPIC\" --bootstrap-server broker:29092 --partitions 3 --replication-factor 1"
  echo "Topic '$TOPIC' created."
else
  echo "Topic '$TOPIC' already exists."
fi

# Show topics
docker exec broker sh -lc "/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --list"