import logging
import sys

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer, FlinkKafkaProducer, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, KafkaSink, KafkaSource)
from pyflink.datastream.formats.json import (JsonRowDeserializationSchema,
                                             JsonRowSerializationSchema)


# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.
def write_to_kafka(env: StreamExecutionEnvironment):
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    ds = env.from_collection(
        [(1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
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


def read_from_kafka(env: StreamExecutionEnvironment):
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_json_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'}
    )
    kafka_consumer.set_start_from_earliest()

    env.add_source(kafka_consumer).print()
    env.execute()


def write_to_kafka_sink(env: StreamExecutionEnvironment):
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    ds = env.from_collection(
        [(1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=type_info)

    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    record_serializer = KafkaRecordSerializationSchema.builder() \
        .set_topic('test_json_topic') \
        .set_value_serialization_schema(serialization_schema) \
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
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()
    kafka_source = (
        KafkaSource.builder()
        .set_topics('test_json_topic')
        .set_value_only_deserializer(deserialization_schema)
        .set_properties({'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'})
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )

    ds = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka source"
    )

    ds.print()
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///absolute-path/to/flink-sql-connector-kafka-3.3.0-1.20.jar")

    print("start writing data to kafka with sink")
    write_to_kafka_sink(env)

    print("start reading data from kafka with source")
    read_from_kafka_source(env)

    print("start writing data to kafka")
    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)
