package Consumers;

/**
 * Created by rishi on 30/10/15.
 */
import Deserializers.MessageDeserializer;
import Models.Message;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SingleThreadHLMessageConsumer {

    private final ConsumerConnector consumer;
    private final String topic;

    public SingleThreadHLMessageConsumer(String zookeeper, String groupId, String topic) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("consumer.timeout.ms", "1000");

        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = topic;
    }

    public long findOffset(long rewindTime, long fileOffset, int partition) throws kafka.consumer.ConsumerTimeoutException{

        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);

        long prevOffset = fileOffset;

        for (final KafkaStream stream : streams) {

            ConsumerIterator<byte[], byte[]> it = stream.iterator();


            while (it.hasNext()) {
                System.out.println("In!");
                MessageDeserializer decoder = new MessageDeserializer(null);
                Message message = (Message) decoder.fromBytes(it.next().message());
                System.out.println("Message from Topic: " + message.getTime());
                System.out.println( it.next().offset());
                System.out.println(it.next().partition());

                if(it.next()==null)
                    return prevOffset;
                if(partition==it.next().partition()) {
                    if (Long.valueOf(message.getTime()).longValue() < rewindTime) {
                        prevOffset = it.next().offset();

                    } else {
                        return prevOffset;
                    }
                }
            }
        }
        if (consumer != null) {
            consumer.shutdown();
        }

        return prevOffset;
    }


}