package kr.co.simplekafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
@SpringBootApplication
public class SimpleKafkaConsumerApplication {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "172.31.102.28:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        SpringApplication.run(SimpleKafkaConsumerApplication.class, args);

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        /* 컨슈머 그룹 기준으로 컨슈머 오프셋을 관리하기 때문에
         subscribe() 메서드를 사용하여 토픽을 구독하는 경우 컨슈머 그룹으로 선언.
         컨슈머가 중단되거나 재시작 되더라도 컨슈머 그룹의 컨슈머 오프셋을 기준으로
         이후 데이터를 처리하기 때문이다.*/
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(configs);
        /*컨슈머에게 토픽을 할당하기 위해 subscribe() 메서드를 사용한다.
        이 메서드는 collection 타입의 String 값들을 받는데, 1개 이상의 토픽 이름을 받을 수 있다.*/
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        /* 
        컨슈머는 poll() 메서드를 호출하여 데이터를 가져와서 처리한다.
        지속적으로 반복 호출 하기 때문에 무한루프 만들어줌
        */
        while (true) {
            /*
            poll() 메서드는 Duration 타입의 인자를 받는데 이 인자 값은 브로커로부터 데이터를 가져올때
            컨슈머 버퍼에 데이터를 기다리기위한 타임아웃 간격을 뜻한다.
            */
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records){
                log.info("레코드 : {}",record);
            }
        }
    }
}
