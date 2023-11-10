package functions;

import java.util.List;

import models.KafkaMessage;

public class GroupedData {
    public String key;
    public List<KafkaMessage> values;

    public GroupedData(String key, List<KafkaMessage> values) {
        this.key = key;
        this.values = values;
    }
}
