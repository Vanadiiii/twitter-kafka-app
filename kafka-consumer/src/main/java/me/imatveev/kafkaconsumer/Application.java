package me.imatveev.kafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import me.imatveev.kafkaconsumer.service.ElasticSearchService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Application {
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String GROUP_ID = "kafka-demo-elasticsearch";
    private static final String TOPIC = "twitter_tweets";


    private static final String URL = "matveev-search-4687614005.us-east-1.bonsaisearch.net";
    private static final String USERNAME = "w93lyjfgc9";
    private static final String PASSWORD = "fo1hz4zxj";

    public static void main(String[] args) throws IOException {
        final ElasticSearchService elasticSearchService = ElasticSearchService.of(
                URL, USERNAME, PASSWORD
        );

        final KafkaConsumer<String, String> consumer = createKafkaConsumer(TOPIC);

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                final String value = record.value();

                log.info("key: {}; value: {}", record.key(), record.value());
                log.info("partition: {}; offset: {}", record.partition(), record.offset());

                final String jsonValue = "{\"line\": \"" + value + "\"}";
                final IndexRequest request = new IndexRequest("twitter", "tweets")
                        .source(jsonValue, XContentType.JSON);

                final IndexResponse response = elasticSearchService.index(request, RequestOptions.DEFAULT);
                final String id = response.getId();

                log.info("elastic response id - {}", id);

                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));

        return consumer;
    }
}
