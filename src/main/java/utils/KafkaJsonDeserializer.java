package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import models.Objeto;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class KafkaJsonDeserializer implements Deserializer {
    private Logger logger = LogManager.getLogManager().getLogger(this.getClass().getName());
    private Class<Objeto> type = Objeto.class;


    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Objeto deserialize(String topic, byte[] data) {
        Objeto retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try{
            retVal = objectMapper.readValue(data,type);
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
