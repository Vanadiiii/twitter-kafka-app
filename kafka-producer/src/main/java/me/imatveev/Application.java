package me.imatveev;

import lombok.extern.slf4j.Slf4j;
import me.imatveev.service.BookLineReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Application {
    private static final String BOOK_PATH_STR = "/home/ivan/IdeaProjects/kafka-application/telegram-bot/twitter-kafka-app/kafka-producer/src/main/resources/Война и мир.txt";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        final KafkaProducer<String, String> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVER);

        final File book = new File(BOOK_PATH_STR);
        final BookLineReader lineReader = new BookLineReader(book);

        readAndSendAll(lineReader, kafkaProducer);
    }

    private static void readAndSendAll(BookLineReader lineReader,
                                       KafkaProducer<String, String> kafkaProducer) {
        log.info("start reading");
        lineReader.doForEveryLineWithDelay(
                line -> {
                    log.info("message - {}", line);
                    kafkaProducer.send(
                            new ProducerRecord<>("twitter_tweets", null, line),
                            (metadata, e) -> {
                                if (e != null) {
                                    log.error("Something bad happened", e);
                                }
                            }
                    );
                },
                5L,
                TimeUnit.SECONDS
        );
    }

    private static KafkaProducer<String, String> createKafkaProducer(String bootstrapServer) {
        final Properties properties = new Properties();

        // create the simplest string-based producer
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // add safety for producer :-)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE.toString()); //add some configs inside)

        // and high throughput
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //add compression
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //increase batch size twice
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //add linger (0 - default)

        return new KafkaProducer<>(properties);
    }
}
