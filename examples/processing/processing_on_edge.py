import json
import logging
import sys
import time
from typing import Iterable

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (KeyedProcessFunction, ProcessWindowFunction,
                                StreamExecutionEnvironment)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee, KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
    KafkaSink, KafkaSource)
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        try:
            raw_value = value if isinstance(value, str) else json.dumps(value)
            payload = json.loads(value) if isinstance(value, str) else value
            ts_field = payload.get("timestamp")
            ts = float(ts_field) if ts_field is not None else 0.0
        except Exception as e:
            print(f"[TimestampAssigner][ERROR] parse error={e} value={value}")
            ts_field = None
            ts = 0.0

        ts_ms = int(ts * 1000) if ts < 1e12 else int(ts)

        now_ms = int(time.time()*1000)

        # stampa di debug utile per capire cosa riceve l'assigner
        print(f"[TimestampAssigner][DEBUG] raw={raw_value} record_ts={record_timestamp} "
              f"ts_field={ts_field} -> ts_ms={ts_ms} / now_ms={now_ms}")

        return ts_ms


class WatermarkLogger(KeyedProcessFunction):
    def process_element(self, value, ctx):
        wm = ctx.timer_service().current_watermark()
        pt = ctx.timer_service().current_processing_time()
        # se value è dict o string, mostralo sintetico
        print(f"[WatermarkLogger] element_license={value.get('license_plate') if isinstance(value, dict) else value} "
              f"current_watermark={wm} processing_time={pt}")
        yield value


# [dict, dict, str, TimeWindow]
class MyProcessWindowFunction(ProcessWindowFunction):
    def process(self, key: str, context: ProcessWindowFunction.Context[TimeWindow], elements: Iterable) -> Iterable:
        total = 0.0
        count = 0
        for element in elements:
            # element is a dict (parsed JSON)
            speed = element.get('collected_metrics', {}).get('speed')
            if speed is None:
                continue

            total += float(speed)
            count += 1

        if count == 0:
            return
        
        avg = total / count

        result = {
            "license_plate": key,
            "window_start": context.window().start,
            "window_end": context.window().end,
            "avg_speed": avg
        }
        print(f"[Window][DEBUG] {result}")   # debug utile

        yield result


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


def read_from_kafka_and_compute(env: StreamExecutionEnvironment):
    kafka_source = (
        KafkaSource.builder()
        .set_topics('test_json_topic')
        .set_value_only_deserializer(SimpleStringSchema())
        .set_properties({'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'})
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )
    
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10)) \
                                        .with_idleness(Duration.of_seconds(10)) \
                                        .with_timestamp_assigner(MyTimestampAssigner())

    # assign watermarks based on the JSON timestamp (with small allowed lateness)
    ds = env.from_source(
        source=kafka_source,
        # WatermarkStrategy.no_watermarks()
        watermark_strategy=watermark_strategy,
        source_name="kafka source"
    )

    # parse JSON strings to dicts (timestamp already assigned by TimestampAssigner)
    # assign_timestamps_and_watermarks(watermark_strategy) \
    ds_parsed = ds.map(lambda s: json.loads(s))

    # ATTENZIONE --> WatermarkLogger per debug
    ds_parsed.key_by(lambda r: r['license_plate'], key_type=Types.STRING()) \
                                .process(WatermarkLogger()) \
                                .print()

    # key by license_plate, sliding window size 60s, slide 5s, compute average speed per window
    ds_windowed_avg = (
        ds_parsed
        .key_by(lambda d: d['license_plate'], key_type=Types.STRING()) \
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1))) \
        .allowed_lateness(time_ms=30000) \
        .process(MyProcessWindowFunction())
    )

    # filter results with avg_speed >= 15
    ds_filtered = ds_windowed_avg.filter(lambda rec: rec.get('avg_speed', 0) >= 15.0)

    # print or sink the final results
    ds_filtered.print()

    env.execute("kafka_sliding_avg_speed")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.get_config().set_auto_watermark_interval(100)  # emit watermark ogni 100 ms
    env.add_jars("file:///absolute-path/to/flink-sql-connector-kafka-3.3.0-1.20.jar")

    print("start writing data to kafka with sink")
    write_to_kafka_sink(env)

    print("start reading data from kafka with source")
    read_from_kafka_and_compute(env)
