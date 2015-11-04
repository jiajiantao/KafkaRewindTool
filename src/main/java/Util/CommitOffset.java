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



    public static void commitPartitionOffsets(String topic, String consumerGroup, Map<Integer, Long> partitionOffsets){

        for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {

            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, entry.getKey());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(entry.getValue(),OffsetAndMetadata.NoMetadata(),-1);
            Map<TopicAndPartition,OffsetAndMetadata> responseInfo = new HashMap<TopicAndPartition, OffsetAndMetadata>();
            responseInfo.put(topicAndPartition,offsetAndMetadata);
            int correlationId = 0;
            OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(consumerGroup,responseInfo,correlationId,consumerGroup);

            SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", 9092, 100000, 64 * 1024,"OffsetCommitConsumer");
            OffsetCommitResponse offsetCommitResponse = simpleConsumer.commitOffsets(offsetCommitRequest);

//            System.out.println(offsetCommitResponse.errorCode(topicAndPartition));
        }

    }
}
