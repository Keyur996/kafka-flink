package jobs;

import functions.GroupByRecordIdAndOrganizationIdProcessFunction;
import functions.GroupedData;
import models.KafkaMessage;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import schema.KafkaMessageSchema;
import schema.KafkaMessageSinkSchema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

@SuppressWarnings({"deprecation", "unused"})
public class StreamingJob {
    private SourceFunction<Long> source;
    private SinkFunction<Long> sink;

    public StreamingJob(SourceFunction<Long> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    public StreamingJob() {
    }

    @SuppressWarnings("unchecked")
    public void execute() throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        KafkaSource<KafkaMessage> kafkaSource = KafkaSource.<KafkaMessage>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("ipoint.public.entity_field_value")
                .setGroupId("entity_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new KafkaMessageSchema())
                .build();


        DataStream<KafkaMessage> stream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)), "Kafka Source").setParallelism(1);

        SerializableTimestampAssigner<KafkaMessage> sz = new SerializableTimestampAssigner<KafkaMessage>() {
            private static final long serialVersionUID = 1L;

            @Override
            public long extractTimestamp(KafkaMessage message, long l) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    Date date = sdf.parse(String.valueOf(message.ts_ms));
                    return date.getTime();
                } catch (ParseException e) {
                    return 0;
                }
            }
        };

        WatermarkStrategy<KafkaMessage> watermarkStrategy = WatermarkStrategy.<KafkaMessage>forBoundedOutOfOrderness(Duration.ofMillis(100)).withTimestampAssigner(sz).withIdleness(Duration.ofSeconds(10));


        DataStream<KafkaMessage> watermarkDataStream = stream.assignTimestampsAndWatermarks(watermarkStrategy);
        DataStream<GroupedData> groupedData = watermarkDataStream.keyBy((KeySelector<KafkaMessage, String>) kafkaMessage -> {
                    Integer recordId = kafkaMessage.after != null ? kafkaMessage.after.record_id : kafkaMessage.before != null ? kafkaMessage.before.record_id : null;
                    return recordId != null ? recordId.toString() : "defaultKey";
                }).window(TumblingProcessingTimeWindows.of(Time.milliseconds(2500),
                        Time.milliseconds(500)))
                .apply(new GroupByRecordIdAndOrganizationIdProcessFunction());

        KafkaSink<GroupedData> sink = KafkaSink.<GroupedData>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(new KafkaMessageSinkSchema("flinkout"))
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        groupedData.sinkTo(sink);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        StreamingJob job = new StreamingJob();
        job.execute();
    }
}