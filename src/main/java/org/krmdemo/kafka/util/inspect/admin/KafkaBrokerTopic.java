package org.krmdemo.kafka.util.inspect.admin;

import lombok.Data;
import lombok.NonNull;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Uuid;

import java.util.*;

import static java.util.Collections.emptySortedMap;

@Data
public class KafkaBrokerTopic {
    private TopicListing topicListing;
    private TopicDescription topicDescription;

    public static class ReconcileInfo {
        SortedMap<String, KafkaBrokerTopic> prevState = emptySortedMap();
        SortedMap<String, KafkaBrokerTopic> nextState = emptySortedMap();
    }

}
