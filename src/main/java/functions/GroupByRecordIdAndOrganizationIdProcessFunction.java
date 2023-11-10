package functions;

import models.KafkaMessage;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class GroupByRecordIdAndOrganizationIdProcessFunction implements WindowFunction<KafkaMessage, GroupedData, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void apply(String key, TimeWindow window, Iterable<KafkaMessage> elements, Collector<GroupedData> out) throws Exception {
        List<KafkaMessage> values = new ArrayList<>();
        for (KafkaMessage element : elements) {
            values.add(element);
        }
        out.collect(new GroupedData(key, values));
    }
}


