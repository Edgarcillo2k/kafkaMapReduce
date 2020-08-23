import models.KafkaConsumerExample;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.function.Consumer;

public class main {
    public static boolean busquedasConsumerStarted = false;
    public static boolean comprasConsumerStarted = false;
    public static void main(String[] args) throws Exception {
        while (true){
            if(!busquedasConsumerStarted){
                busquedasConsumerStarted = true;
                try {
                    runConsumer("busquedas");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(!comprasConsumerStarted){
                comprasConsumerStarted = true;
                try {
                    runConsumer("compras");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static void runConsumer(String topic) throws InterruptedException {
        final Consumer<Long, String> consumer = KafkaConsumerExample.createConsumer(topic);
        final int giveUp = 100;
        int noRecordsCount = 0;
        while (true) {
            System.out.println("Consumer esperando registros");
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            System.out.println("Registros obtenidos");
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
        if (topic.equals("busquedas")) {
            busquedasConsumerStarted = false;
        }
        else{
            comprasConsumerStarted = false;
        }
        System.out.println("DONE");
        if(noRecordsCount>0){
            try {
                StartJobMain.main(new String[0]);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}