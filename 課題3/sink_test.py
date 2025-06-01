#!/usr/bin/env python3
# sink_test.py

import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
    DeliveryGuarantee,
)

# ── 設定 ─────────────────────────────────────────────
STUDENT_ID      = "s2410224"
INPUT_TOPIC     = "i483-allsensors"
OUTPUT_TOPIC    = "i483-allsensors"
# ───────────────────────────────────────────────────────

def parse_flat(raw: str):
    """
    raw: "i483-sensors-s2410224-SCD41-co2,ts,value"
    → [(sensor, dtype, ts, value)] または []
    """
    try:
        topic, ts_str, val_str = raw.split(",", 2)
        parts = topic.split("-")
        if len(parts) == 5 and parts[2] == STUDENT_ID:
            sensor = parts[3]
            dtype  = parts[4]
            return [(sensor, dtype, int(ts_str), float(val_str))]
    except:
        pass
    return []

class StatsPerKey(ProcessWindowFunction):
    def process(self, key, context, elements):
        """
        key      = (sensor, dtype)
        elements = list of (sensor, dtype, ts, value)
        出力    = ["topic,value", ...]
        """
        sensor, dtype = key
        vals = [e[3] for e in elements]
        if not vals:
            return []
        outputs = []
        for metric, v in [("min", min(vals)), ("max", max(vals)), ("avg", sum(vals)/len(vals))]:
            topic_out = f"i483-sensors-{STUDENT_ID}-analytics-{sensor}_{metric}-{dtype}"
            outputs.append(f"{topic_out},{v:.2f}")
        return outputs

def main():
    env = StreamExecutionEnvironment.get_execution_environment(Configuration())
    env.set_parallelism(1)

    # 1) KafkaSource (入力)
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
        WatermarkStrategy
          .for_monotonous_timestamps()
          .with_timestamp_assigner(lambda raw, _: int(raw.split(",",2)[1])),
        "kafka_source"
    )

    # 2) validate + flatMap でフィルタ＆パース
    parsed = stream.flat_map(
        parse_flat,
        output_type=Types.TUPLE([
            Types.STRING(),  # sensor
            Types.STRING(),  # dtype
            Types.LONG(),    # timestamp
            Types.DOUBLE()   # value
        ])
    )

    # 3) ウィンドウ集計（5 分間のデータを 30 秒ごとに集計）
    stats = (
        parsed
        .key_by(lambda rec: (rec[0], rec[1]))
        .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
        .process(StatsPerKey(), output_type=Types.STRING())
    )

    # 4) KafkaSink (出力: topic,value → i483-fvtt)
    sink = (
        KafkaSink
        .builder()
        .set_bootstrap_servers("150.65.230.59:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(OUTPUT_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    stats.sink_to(sink)

    env.execute("Flink All-Sensors Stats to i483-fvtt")

if __name__ == "__main__":
    main()

