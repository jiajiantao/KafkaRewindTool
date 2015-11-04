package Consumers;

import Deserializers.MessageDeserializer;
import Models.Message;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;

public class OffsetWithinFileConsumer {

    public static long run(String topic, long rewindTime, long fileOffset, int partition, String leaderHost, int leaderPort) {

        SimpleConsumer consumer = new SimpleConsumer(leaderHost, leaderPort, 1000000, 64 * 1024, "OffsetWithinFileConsumer");
        long readOffset = fileOffset;

        int numErrors = 0;


        long prevOffset = fileOffset;

        long reads = 200000;
        while(reads>0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leaderHost, leaderPort, 1000000, 64 * 1024, "OffsetWithinFileConsumer");
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId("OffsetWithinFileConsumer")
                    .addFetch(topic, partition, readOffset, 1000000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leaderHost + " Reason: " + code);
                if (numErrors > 5)
                    ;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    //For simple case ask for the last element to reset
                    readOffset = fileOffset;

                }
                consumer.close();
                consumer = null;

            }
            numErrors = 0;


            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {


                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                MessageDeserializer decoder = new MessageDeserializer(null);
                Message message = (Message) decoder.fromBytes(bytes);

                if (rewindTime - Long.valueOf(message.getTime()) >= 0) {
                    prevOffset = messageAndOffset.offset();
                } else {

                    System.out.println("Timestamp: " + message.getTime());
                    return prevOffset;
                }

                numRead++;
                reads--;

            }
        }

        if (consumer != null) consumer.close();

        return prevOffset;

    }

}