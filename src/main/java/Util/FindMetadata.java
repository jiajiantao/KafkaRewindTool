package Util; /**
 * Created by rishi on 2/11/15.
 */

import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.List;

public class FindMetadata {


    public static TopicMetadataResponse findPartitionInfo(String topic) {

        SimpleConsumer simpleConsumer = new SimpleConsumer("rishi-Latitude-E7250", 9093, 100000, 64*1024, "PartitonInfoFinderClient");
        List<String> topics = new ArrayList<String>();
        topics.add(topic);

        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
        TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

//        System.out.println(topicMetadataResponse.topicsMetadata().get(0).partitionsMetadata().get(1).leader().);
        return topicMetadataResponse;



    }
}

