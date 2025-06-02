#!/usr/bin/env python3
# analyse_processing_time.py：プロセスタイムでセンサ集計（課題3）

from pyflink.common import Configuration, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee, KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
    KafkaSink, KafkaSource)
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import SlidingProcessingTimeWindows

# ────────────── 設定 ──────────────
INPUT_TOPIC = "i483-allsensors"
OUTPUT_TOPIC = "i483-fvtt"
FIXED_ANALYTICS_PREFIX = "i483-sensors-s2410040-analytics-"
# ─────────────────────────────────


def parse_flat(raw: str):
    try:
        topic, ts_str, val_str = raw.split(",", 2)
        parts = topic.split("-")
        if len(parts) == 5 and parts[2].startswith("s"):
            input_student = parts[2]
            sensor = parts[3].upper()
            dtype = parts[4].lower()
            ts = int(ts_str)
            val = float(val_str)
            return [(input_student, sensor, dtype, ts, val)]
    except Exception as e:
        print(f"[ERROR] parse_flat: {e} ← {raw}")
    return []


class StatsPerKey(ProcessWindowFunction):
    def process(self, key, context, elements):
        input_student, sensor, dtype = key
        vals = [e[4] for e in elements]
        if not vals:
            return []

        outputs = []
        for metric, v in [
            ("min", min(vals)),
            ("max", max(vals)),
            ("avg", sum(vals) / len(vals))
        ]:
            analytics_topic = (
                f"{FIXED_ANALYTICS_PREFIX}"
                f"{input_student}_{sensor}_{metric}-{dtype}"
            )
            value_str = f"{v:.2f}"
            outputs.append(f"{analytics_topic},{value_str}")
        return outputs


def main():
    env = StreamExecutionEnvironment.get_execution_environment(Configuration())
    env.set_parallelism(1)

    # Kafka ソース（プロセスタイム → WatermarkStrategy.no_watermarks()）
    source = (
        KafkaSource
        .builder()
        .set_bootstrap_servers("150.65.230.59:9092")
        .set_topics(INPUT_TOPIC)
        .set_value_only_deserializer(SimpleStringSchema())
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),  # プロセスタイム処理に必要
        "kafka_source"
    )

    parsed = stream.flat_map(
        parse_flat,
        output_type=Types.TUPLE([
            Types.STRING(),  # 学籍番号
            Types.STRING(),  # センサ名
            Types.STRING(),  # データ種別
            Types.LONG(),    # ts（使用しないが保持）
            Types.DOUBLE(),  # センサ値
        ])
    )

    stats = (
        parsed
        .key_by(lambda rec: (rec[0], rec[1], rec[2]))  # key = (学籍番号, センサ, データ種別)
        .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(30)))
        .process(StatsPerKey(), output_type=Types.STRING())
    )

    record_serializer = (
        KafkaRecordSerializationSchema
        .builder()
        .set_topic(OUTPUT_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink
        .builder()
        .set_bootstrap_servers("150.65.230.59:9092")
        .set_record_serializer(record_serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    stats.sink_to(sink)
    env.execute("ProcessingTime All-Sensor Stats")


if __name__ == "__main__":
    main()