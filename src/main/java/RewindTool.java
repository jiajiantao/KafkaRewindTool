import Util.CommitOffset;
import Util.FindMetadata;
import Util.FindOffset;
import kafka.javaapi.TopicMetadataResponse;

import java.util.Map;


/**
 * Created by rishi on 2/11/15.
 */
public class RewindTool {

    private String consumerGroup ;
    private String topic ;
    private  String topicHost ;
    private  Integer topicPort ;
    private  Long rewindTime;

    public RewindTool(String consumerGroup, String topic, String topicHost, Integer topicPort, Long rewindTime) {
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.topicHost = topicHost;
        this.topicPort = topicPort;
        this.rewindTime = rewindTime;
    }

    public void runRewindTool(){
        TopicMetadataResponse topicMetadataResponse = FindMetadata.findPartitionInfo(topic,topicHost,topicPort);

        Map<Integer,Long> partitionOffsets = FindOffset.getOffset(topic,rewindTime, consumerGroup ,topicMetadataResponse);
        CommitOffset.commitPartitionOffsets(topic,topicHost,topicPort,consumerGroup,partitionOffsets);
        System.out.println(partitionOffsets);

    }

    public static void main(String[] args) {
        RewindTool rewindTool = new RewindTool("testgroup","TestMessagesFinal", "rishi-Latitude-E7250", 9093,1447584561238L);
        rewindTool.runRewindTool();

    }

}
