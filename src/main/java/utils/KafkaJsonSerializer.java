package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class KafkaJsonSerializer implements Serializer {
    private Logger logger = LogManager.getLogManager().getLogger(this.getClass().getName());

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try{
            retVal = objectMapper.writeValueAsBytes(data);
        }
        catch(Exception e){
            logger.log(Level.WARNING,e.getMessage());
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
