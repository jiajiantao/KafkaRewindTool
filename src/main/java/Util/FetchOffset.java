package Util;

import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rishi on 16/11/15.
 */
public class FetchOffset {
    public static long fetchOffset(String topic, String topicHost, int topicPort, int partition, String consumerGroup){

        short versionID = kafka.api.OffsetRequest.CurrentVersion();
        int correlationId = 0;
        List<TopicAndPartition> topicPartitionList = new ArrayList<TopicAndPartition>();
        TopicAndPartition myTopicAndPartition = new TopicAndPartition(topic , partition );
        topicPartitionList.add(myTopicAndPartition);
        OffsetFetchRequest offsetFetchReq = new OffsetFetchRequest(
                consumerGroup, topicPartitionList, versionID, correlationId, consumerGroup);

        SimpleConsumer simpleConsumer = new SimpleConsumer(topicHost, topicPort, 100000, 64 * 1024, "FetchOffsetConsumer");

        OffsetFetchResponse offsetFetchResponse = simpleConsumer.fetchOffsets(offsetFetchReq);

        long currentOffset = offsetFetchResponse.offsets().get(myTopicAndPartition).offset();
        return currentOffset;

    }
}
