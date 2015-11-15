import Util.CommitOffset;
import Util.FindMetadata;
import Util.FindOffset;
import kafka.javaapi.TopicMetadataResponse;

import java.util.Map;


/**
 * Created by rishi on 2/11/15.
 */
public class TestRewindTool {

    public static final String consumerGroup = "testgroup";
    public static final String topic = "TestMessagesFinal";
    public static final String topicHost = "rishi-Latitude-E7250";
    public static final Integer topicPort = 9093;
    public static final Long rewindTime = 1447584559934L;

    public static void runRewindTool(){
        TopicMetadataResponse topicMetadataResponse = FindMetadata.findPartitionInfo(topic,topicHost,topicPort);

        Map<Integer,Long> partitionOffsets = FindOffset.getOffset(topic,rewindTime, consumerGroup ,topicMetadataResponse);
        CommitOffset.commitPartitionOffsets(topic,consumerGroup,partitionOffsets);
        System.out.println(partitionOffsets);

    }

    public static void main(String[] args) {
        runRewindTool();

    }

}
