docker build -t data-generating-process-to-kafka:0.1.0 -t data-generating-process-to-kafka:latest .

# If Kafka runs on your host, use host.docker.internal on macOS:
docker run --rm -p 8000:8000 \
  -e KAFKA_BOOTSTRAP=host.docker.internal:9092 \
  sdv-kafka:latest