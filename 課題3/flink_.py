import re

from pyflink.common import Configuration, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee, KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
    KafkaSink, KafkaSource)
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows

# ── 設定 ─────────────────────────────────────────────
STUDENT_ID      = "s2410040"
INPUT_TOPICS    = [
    f"i483-sensors-{STUDENT_ID}-BH1750-illumination",
    f"i483-sensors-{STUDENT_ID}-SCD41-co2"
]
OUTPUT_TOPIC    = "i483-allsensors"
# ───────────────────────────────────────────────────────

def parse_flat(raw: str):
    """
    raw: "i483-sensors-s2410040-SCD41-co2,ts,value"
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
            value_out = f"{v:.2f}"
            # ← ここに DEBUG PRINT を追加！
            print(f"[DEBUG] Emitting to {topic_out}: {value_out}")
            outputs.append(f"{topic_out},{value_out}")
        return outputs

def main():
    env = StreamExecutionEnvironment.get_execution_environment(Configuration())
    env.set_parallelism(1)

    # 1) KafkaSource (入力)
    source = (
        KafkaSource
        .builder()
        .set_bootstrap_servers("150.65.230.59:9092")
        .set_topics(*INPUT_TOPICS)   # ← 明示的に複数トピック指定
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

    # 4) KafkaSink (出力)
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

    env.execute("Flink Sensors Analytics Job")

if __name__ == "__main__":
    main()
