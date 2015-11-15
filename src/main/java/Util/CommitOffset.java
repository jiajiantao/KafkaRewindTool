package Util;

import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishi on 2/11/15.
 */
public class CommitOffset {


    public static void commitPartitionOffsets(String topic, String topicHost, Integer topicPort, String consumerGroup, Map<Integer, Long> partitionOffsets) {

        Map<Integer, Long> oldPartitionOffsets = getOldOffsets(topic,topicHost,topicPort,consumerGroup,partitionOffsets);

        for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, entry.getKey());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(entry.getValue(), OffsetAndMetadata.NoMetadata(), -1);
            Map<TopicAndPartition, OffsetAndMetadata> responseInfo = new HashMap<TopicAndPartition, OffsetAndMetadata>();
            responseInfo.put(topicAndPartition, offsetAndMetadata);
            int correlationId = 0;
            OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(consumerGroup, responseInfo, correlationId, consumerGroup);

            SimpleConsumer simpleConsumer = new SimpleConsumer(topicHost, topicPort, 100000, 64 * 1024, "OffsetCommitConsumer");
            OffsetCommitResponse offsetCommitResponse = simpleConsumer.commitOffsets(offsetCommitRequest);

            //In case of error rollback
            if(offsetCommitResponse.errorCode(topicAndPartition)!=0){
                for (Map.Entry<Integer, Long> entry2 : oldPartitionOffsets.entrySet()) {
                    if(entry2.getKey()!=entry.getKey()){
                        TopicAndPartition topicAndPartition2 = new TopicAndPartition(topic, entry2.getKey());
                        OffsetAndMetadata offsetAndMetadata2 = new OffsetAndMetadata(entry2.getValue(), OffsetAndMetadata.NoMetadata(), -1);
                        Map<TopicAndPartition, OffsetAndMetadata> responseInfo2 = new HashMap<TopicAndPartition, OffsetAndMetadata>();
                        responseInfo.put(topicAndPartition2, offsetAndMetadata2);
                        OffsetCommitRequest offsetCommitRequest2 = new OffsetCommitRequest(consumerGroup, responseInfo, correlationId, consumerGroup);

                        SimpleConsumer simpleConsumer2 = new SimpleConsumer(topicHost, topicPort, 100000, 64 * 1024, "OffsetCommitConsumer");
                        OffsetCommitResponse offsetCommitResponse2 = simpleConsumer.commitOffsets(offsetCommitRequest);


                    }
                    else
                        break;

                }
                break;
            }
         }

    }


    private static Map<Integer, Long> getOldOffsets(String topic, String topicHost, Integer topicPort, String consumerGroup, Map<Integer, Long> partitionOffsets) {

        Map<Integer, Long> oldPartitionOffsets = new HashMap<Integer, Long>();
        for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {

            long oldOffset = FetchOffset.fetchOffset(topic,topicHost,topicPort,entry.getKey(),consumerGroup);
            oldPartitionOffsets.put(entry.getKey(), oldOffset);
        }


        return oldPartitionOffsets;
    }

}
