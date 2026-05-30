import logging
import sys
import json

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, KafkaSink, KafkaSource)


def write_to_kafka_sink(env: StreamExecutionEnvironment):
    type_info = Types.STRING()

    ds = env.from_collection(
        [
            '{"license_plate": "QZ321BX", "timestamp": 1779961308.642776, "collected_metrics": {"gps": {"latitude": 44.49321, "longitude": 11.27662}, "speed": 10.0, "tyre_pressure": 4.5, "brake_status": "eccellente", "engine_status": "eccellente", "num_psg": 10, "environmental": {"temperature": 4.501, "humidity": 20.336}, "electric": {"battery_level": 100.0, "battery_temperature": 25.0} }}',
            '{"license_plate": "DS881YU", "timestamp": 1780150605.1027389, "collected_metrics": {"gps": {"latitude": 44.49341, "longitude": 11.27682}, "speed": 36.9, "tyre_pressure": 4.45, "brake_status": "eccellente", "engine_status": "ottimo", "num_psg": 10, "environmental": {"temperature": 18.215, "humidity": 34.024}, "hybrid": {"battery_level": 94.29, "battery_temperature": 27.13, "fuel_level": 399.4, "fuel_consumption": 0.6} }}',
            '{"license_plate": "BV207AS", "timestamp": 1780150605.102654, "collected_metrics": {"gps": {"latitude": 44.49341, "longitude": 11.27682}, "speed": 22.5, "tyre_pressure": 4.31, "brake_status": "accettabile", "engine_status": "ottimo", "num_psg": 10, "environmental": {"temperature": 11.883, "humidity": 66.181}, "termic": {"fuel_level": 479.2, "fuel_consumption": 0.8} }}',
            '{"license_plate": "QZ321BX", "timestamp": 1779961308.642776, "collected_metrics": {"gps": {"latitude": 44.49321, "longitude": 11.27662}, "speed": 15.0, "tyre_pressure": 4.5, "brake_status": "eccellente", "engine_status": "eccellente", "num_psg": 10, "environmental": {"temperature": 4.501, "humidity": 20.336}, "electric": {"battery_level": 100.0, "battery_temperature": 25.0} }}',
            '{"license_plate": "DS881YU", "timestamp": 1780150605.1027389, "collected_metrics": {"gps": {"latitude": 44.49341, "longitude": 11.27682}, "speed": 30.9, "tyre_pressure": 4.45, "brake_status": "eccellente", "engine_status": "ottimo", "num_psg": 10, "environmental": {"temperature": 18.215, "humidity": 34.024}, "hybrid": {"battery_level": 94.29, "battery_temperature": 27.13, "fuel_level": 399.4, "fuel_consumption": 0.6} }}',
            '{"license_plate": "BV207AS", "timestamp": 1780150605.102654, "collected_metrics": {"gps": {"latitude": 44.49341, "longitude": 11.27682}, "speed": 25.5, "tyre_pressure": 4.31, "brake_status": "accettabile", "engine_status": "ottimo", "num_psg": 10, "environmental": {"temperature": 11.883, "humidity": 66.181}, "termic": {"fuel_level": 479.2, "fuel_consumption": 0.8} }}'
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
        .build()
    )

    # note that the output type of ds must be RowTypeInfo
    ds.sink_to(kafka_sink)
    env.execute()


def read_from_kafka_source(env: StreamExecutionEnvironment):    
    kafka_source = (
        KafkaSource.builder()
        .set_topics('test_json_topic')
        .set_value_only_deserializer(SimpleStringSchema())
        .set_properties({'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'})
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )

    ds = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka source"
    )

    ds_mapped_json = ds.map(lambda data: json.loads(data))
    # key_by(lambda data: data['license_plate'], key_type=Types.STRING()) \
    ds_filt_mapped = ds_mapped_json.filter(lambda data: data['collected_metrics']['speed'] > 30.0) \
                                    .map(lambda data: { "license_plate": data['license_plate'],
                                                        "timestamp": data['timestamp'], 
                                                        "collected_metrics": {
                                                            "gps": data['collected_metrics']['gps'], 
                                                            "speed": data['collected_metrics']['speed']
                                                            }
                                                        })
    
    ds_filt_mapped.print()
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///absolute-path/to/flink-sql-connector-kafka-3.3.0-1.20.jar")

    print("start writing data to kafka with sink")
    write_to_kafka_sink(env)

    print("start reading data from kafka with source")
    read_from_kafka_source(env)
