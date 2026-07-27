#!/bin/bash
set -e

echo "Step 1: Navigate and stop"
cd ~/UniMaaS/Adient-T4.2/Basyx
docker compose down kafka-aas-bridge --remove-orphans

echo "Step 2: Clean Docker cache"
docker system prune -af --volumes

echo "Step 3: Rebuild"
cd ~/UniMaaS/Adient-T4.2/kafka_basyx_bridge
docker build --no-cache -t kafka-basyx-bridge:fixed .

echo "Step 4: Start"
cd ~/UniMaaS/Adient-T4.2/Basyx
docker compose up -d kafka-aas-bridge
sleep 5

echo "Step 5: Verify code"
docker exec kafka-aas-bridge grep -A 2 "if isinstance(value, (int, float)):" /app/consumer.py

echo "Step 6: Test"
docker run --rm --network security_not_edge curlimages/curl curl -s -X POST http://device-magnum:8081/device/action/triggerlifecycleupdate \
  -H "Content-Type: application/json" \
  -d '{"input":{"status": "in_use", "numberOfUses": 5, "newLocation": "site_12", "carbonFootprintIncrement": 3.42}}'

sleep 2

echo "Step 7: Check results"
docker logs kafka-aas-bridge --tail 30 | grep "AAS 1114567:"