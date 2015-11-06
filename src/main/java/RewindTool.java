import Util.CommitOffset;
import Util.FindMetadata;
import Util.FindOffset;
import kafka.javaapi.TopicMetadataResponse;

import java.util.Map;


/**
 * Created by rishi on 2/11/15.
 */
public class RewindTool {

    public static final String consumerGroup = "testgroup";
    public static final String topic = "TestMessagesFinal";
    public static final Long rewindTime = 1444129970000L;

    public static void main(String[] args) {

        TopicMetadataResponse topicMetadataResponse = FindMetadata.findPartitionInfo(topic);

        Map<Integer,Long> partitionOffsets = FindOffset.getOffset(topic,rewindTime, consumerGroup ,topicMetadataResponse);
        CommitOffset.commitPartitionOffsets(topic,consumerGroup,partitionOffsets);
        System.out.println(partitionOffsets);

    }

}
