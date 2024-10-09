package kr.co.simplekafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootApplication
public class AdminAPI {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "172.31.102.28:9092";
    private final static String GROUP_ID = "test-group";
    private final static int PARTITION_NUMBER = 0;

//    컨슈머의 안전한 종료

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SpringApplication.run(AdminAPI.class, args);

        Properties configs = new Properties();

        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        AdminClient admin = AdminClient.create(configs);
        log.info("== Get broker information");
        for (Node node : admin.describeCluster().nodes().get()){
            log.info("node : {}" , node);

            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
            describeConfigs.all().get().forEach((broker, config) -> {
                config.entries().forEach(configEntry -> log.info("{}= {}", configEntry.name(), configEntry.value()));
            });
        }

    }
}
