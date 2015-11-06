package Util;

import Consumers.OffsetWithinFileConsumer;
import kafka.javaapi.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rishi on 3/11/15.
 */
public class FindOffset {

    private static long getFileOffset(SimpleConsumer findOffsetConsumer,String topic, Long rewindTime, String consumerGroup, Integer partition){

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(rewindTime, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumerGroup);
        OffsetResponse offsetResponse = findOffsetConsumer.getOffsetsBefore(offsetRequest);

        if (offsetResponse.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + offsetResponse.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = offsetResponse.offsets(topic, partition);
        long fileOffset=0;
        try{
            fileOffset = offsets[0];
        }
        catch (ArrayIndexOutOfBoundsException exception){
            fileOffset=0;
        }
        finally {
            return fileOffset;
        }
    }

    private static long getOffsetWithinFile(String topic, long rewindTime,long fileOffset, int partition,String leaderHost, int leaderPort){


        long offset=fileOffset;
        offset = OffsetWithinFileConsumer.run(topic, rewindTime, fileOffset, partition, leaderHost, leaderPort);
        System.out.println("Offset Within File: "+ offset);

        return offset;

    }

    public static Map<Integer,Long> getOffset(String topic, Long rewindTime, String consumerGroup, TopicMetadataResponse topicMetadataResponse){



        Map<Integer , Long > partitionOffsets = new HashMap<Integer, Long>();
        for (int i = 0; i < topicMetadataResponse.topicsMetadata().get(0).partitionsMetadata().size(); i++) {

            SimpleConsumer findOffsetConsumer = new SimpleConsumer("localhost", topicMetadataResponse.topicsMetadata().get(0).partitionsMetadata().get(i).leader().port(), 100000, 64*1024, "FindOffsetConsumer");

            long fileOffset = getFileOffset(findOffsetConsumer,topic,rewindTime,consumerGroup,topicMetadataResponse.topicsMetadata().get(0).partitionsMetadata().get(i).partitionId());

            int partition = topicMetadataResponse.topicsMetadata().get(0).partitionsMetadata().get(i).partitionId();
            String leaderHost = topicMetadataResponse.topicsMetadata().get(0).partitionsMetadata().get(i).leader().host();
            int leaderPort = topicMetadataResponse.topicsMetadata().get(0).partitionsMetadata().get(i).leader().port();
            long offset = getOffsetWithinFile(topic, rewindTime,fileOffset, partition, leaderHost,leaderPort );

            partitionOffsets.put(topicMetadataResponse.topicsMetadata().get(0).partitionsMetadata().get(i).partitionId() ,offset);
        }

        return partitionOffsets;

    }
}
