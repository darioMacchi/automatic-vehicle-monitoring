echo Starting Kafka Broker...
KAFKA_CLUSTER_ID="AVM-cluster-001"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server-2.properties
bin/kafka-server-start.sh config/server-2.properties
