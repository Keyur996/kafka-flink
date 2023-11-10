package functions;

import models.KafkaMessage;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.List;

public class GroupByRecordIdAndOrganizationIdProcessFunction extends ProcessWindowFunction<KafkaMessage, GroupedData, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String key, ProcessWindowFunction<KafkaMessage, GroupedData, String, TimeWindow>.Context context,
                        Iterable<KafkaMessage> elements, org.apache.flink.util.Collector<GroupedData> out) throws Exception {
        List<KafkaMessage> values = new ArrayList<>();
        for (KafkaMessage element : elements) {
            values.add(element);
        }
        out.collect(new GroupedData(key, values));
    }
}

