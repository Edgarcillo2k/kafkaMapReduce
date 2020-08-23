import models.KafkaConsumerExample;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class main {
    public static boolean busquedasConsumerStarted = false;
    public static boolean comprasConsumerStarted = false;
    public static void main(String[] args) throws Exception {
        ExtendsThread thread =  new ExtendsThread("busquedas");
        ExtendsThread thread2 = new ExtendsThread("compras");
        thread.start();
        thread2.start();
        System.out.println("Corrio");
        System.exit(0);
    }
}
