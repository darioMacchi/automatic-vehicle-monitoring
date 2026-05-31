import logging
import sys
import time

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee, FlinkKafkaProducer, KafkaRecordSerializationSchema,
    KafkaSink)
from pyflink.datastream.formats.json import JsonRowSerializationSchema


# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.
def write_to_kafka(env: StreamExecutionEnvironment):
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    ds = env.from_collection(
        [(7, 'Hello from Dario!'), (8, 'Hello from Rita!'), (9, 'Hello from Stefano!')],
        type_info=type_info)

    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    kafka_producer = FlinkKafkaProducer(
        topic='test_json_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute()


def write_to_kafka_sink(env: StreamExecutionEnvironment):
    type_info = Types.STRING()

    now_ms = int(time.time() * 1000)
    ts_old = (now_ms-2000)/1000.0
    ts_new = (now_ms-1000)/1000.0

    ds = env.from_collection(
        [
            '{"license_plate": "BV207AS", "timestamp":' + str(ts_old) + ',"collected_metrics": {"gps": {"latitude": 44.49321, "longitude": 11.27662}, "speed": 10.0, "tyre_pressure": 4.5, "brake_status": "eccellente", "engine_status": "eccellente", "num_psg": 10, "environmental": {"temperature": 4.609, "humidity": 96.772}, "termic": {"fuel_level": 480.0, "fuel_consumption": 0.0} }}',
            '{"license_plate": "VN124HB", "timestamp":' + str(ts_old) + ',"collected_metrics": {"gps": {"latitude": 44.49321, "longitude": 11.27662}, "speed": 10.0, "tyre_pressure": 4.5, "brake_status": "eccellente", "engine_status": "eccellente", "num_psg": 10, "environmental": {"temperature": 8.994, "humidity": 92.191}, "hybrid": {"battery_level": 100.0, "battery_temperature": 25.0, "fuel_level": 400.0, "fuel_consumption": 0.0} }}',
            '{"license_plate": "KP606QR", "timestamp":' + str(ts_old) + ',"collected_metrics": {"gps": {"latitude": 44.49341, "longitude": 11.27662}, "speed": 10.0, "tyre_pressure": 4.5, "brake_status": "eccellente", "engine_status": "eccellente", "num_psg": 10, "environmental": {"temperature": 3.944, "humidity": 10.77}, "electric": {"battery_level": 100.0, "battery_temperature": 25.0} }}',
            '{"license_plate": "BV207AS", "timestamp":' + str(ts_new) + ',"collected_metrics": {"gps": {"latitude": 44.49326, "longitude": 11.27667}, "speed": 29.2, "tyre_pressure": 4.39, "brake_status": "eccellente", "engine_status": "ottimo", "num_psg": 10, "environmental": {"temperature": 2.592, "humidity": 90.182}, "termic": {"fuel_level": 479.8, "fuel_consumption": 0.2} }}',
            '{"license_plate": "VN124HB", "timestamp":' + str(ts_new) + ',"collected_metrics": {"gps": {"latitude": 44.49326, "longitude": 11.27667}, "speed": 19.4, "tyre_pressure": 4.48, "brake_status": "eccellente", "engine_status": "ottimo", "num_psg": 10, "environmental": {"temperature": 9.886, "humidity": 88.054}, "hybrid": {"battery_level": 90.57, "battery_temperature": 25.27, "fuel_level": 399.85, "fuel_consumption": 0.15} }}',
            '{"license_plate": "KP606QR", "timestamp":' + str(ts_new) + ',"collected_metrics": {"gps": {"latitude": 44.49326, "longitude": 11.27667}, "speed": 18.4, "tyre_pressure": 4.38, "brake_status": "ottimo", "engine_status": "ottimo", "num_psg": 10, "environmental": {"temperature": 2.548, "humidity": 13.657}, "electric": {"battery_level": 99.9, "battery_temperature": 23.78} }}'
        ],
        type_info=type_info
    )
    
    record_serializer = KafkaRecordSerializationSchema.builder() \
        .set_topic('test_json_topic') \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()
    
    kafka_sink = (
        KafkaSink.builder()
        .set_record_serializer(record_serializer)
        .set_bootstrap_servers('localhost:9092')
        .set_property("group.id", "test_group")
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # note that the output type of ds must be RowTypeInfo
    ds.sink_to(kafka_sink)
    env.execute("kafka_sinking_events")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///absolute-path/to/flink-sql-connector-kafka-3.3.0-1.20.jar")

    print("start writing data to kafka")
    write_to_kafka_sink(env)
