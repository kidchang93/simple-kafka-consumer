package kr.co.simplekafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@SpringBootApplication
public class SyncOffsetCommitShutdownhook {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "172.31.102.28:9092";
    private final static String GROUP_ID = "test-group";
    private final static int PARTITION_NUMBER = 0;

//    컨슈머의 안전한 종료

    public static void main(String[] args) {
        SpringApplication.run(SyncOffsetCommitShutdownhook.class, args);
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(configs);
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        consumer.subscribe(List.of(TOPIC_NAME));
        try {
            while (true) {
            
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records){
                    log.info("레코드 : {}",record);
                }

            }
        }catch (WakeupException e){
            log.warn("WakeUp Consumer");
            // 리소스 종료 처리
        } finally {
            consumer.close();
        }

    }
}
