package Deserializers;

/**
 * Created by rishi on 30/10/15.
 */

import Models.Message;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.codehaus.jackson.map.ObjectMapper;


public class MessageDeserializer implements Decoder<Object> {

    public MessageDeserializer(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }


    public Object fromBytes(byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(bytes, Message.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}