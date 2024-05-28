package org.krmdemo.kafka.util.inspect.admin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NonNull;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Uuid;

import java.util.*;

import static java.util.Collections.emptySortedMap;

@Data
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBrokerTopic {
    private TopicDescription topicDescription;
    private List<Integer> partitions;

    public static class ReconcileInfo {
        SortedMap<String, KafkaBrokerTopic> prevState = emptySortedMap();
        SortedMap<String, KafkaBrokerTopic> nextState = emptySortedMap();
    }

}
