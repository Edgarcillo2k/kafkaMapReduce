import models.KafkaConsumerExample;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;


class ExtendsThread extends Thread{
    private String topic;

    public ExtendsThread(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        while(true) {
            Consumer<Long, String> consumer = KafkaConsumerExample.createConsumer(topic);
            int giveUp = 100;
            int noRecordsCount = 0;
            while (true) {
                System.out.println("Consumer esperando registros");
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
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
                main.busquedasConsumerStarted = false;
            } else {
                main.comprasConsumerStarted = false;
            }
            System.out.println("DONE");
            if (noRecordsCount > 0) {
                try {
                    StartJobMain.main(new String[0]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
