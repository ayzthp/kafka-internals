#!/usr/bin/env bash
# Create a replicated topic, append keyed records, and print where they landed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BOOTSTRAP="kafka-1:19092"
TOPIC="orders"

echo "Waiting for the controller quorum..."
for _ in $(seq 1 60); do
  if docker compose exec -T kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2

echo
echo "Appending keyed events. The key (before the colon) chooses the partition."
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --property parse.key=true \
  --property key.separator=: <<'EOF'
alice:{"event":"add_to_cart","sku":"mug"}
bob:{"event":"add_to_cart","sku":"book"}
alice:{"event":"checkout","sku":"mug"}
carol:{"event":"view","sku":"lamp"}
bob:{"event":"checkout","sku":"book"}
EOF

echo
echo "Partition leaders, replicas, and the in-sync set:"
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --describe --topic "$TOPIC"

echo
echo "Records, with the partition and offset the leader assigned:"
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 5 \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true \
  --formatter-property print.timestamp=true
