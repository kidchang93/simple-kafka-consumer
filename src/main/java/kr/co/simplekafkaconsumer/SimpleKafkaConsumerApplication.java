package kr.co.simplekafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.*;

@Slf4j
@SpringBootApplication
public class SimpleKafkaConsumerApplication {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "172.31.102.28:9092";
    private final static String GROUP_ID = "test-group";
    private final static int PARTITION_NUMBER = 0;


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
//        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 리밸런스 발생시 수동 커밋을 위해 false로 설정.
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(configs);
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
//        컨슈머에게 토픽을 할당하기 위해 subscribe() 메서드를 사용한다.
//        이 메서드는 collection 타입의 String 값들을 받는데, 1개 이상의 토픽 이름을 받을 수 있다.

        /**
         * 파티션 할당 컨슈머
         * subscribe 대신 assign을 쓰면 PARTITION_NUMBER 에 할당된다. 리밸런싱 과정 X
         */
//        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));
//        consumer.subscribe(Arrays.asList(TOPIC_NAME));
//        리밸런스 방식시 사용하는 subscribe
//        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());

        /**
         * 컨슈머에 할당된 파티션확인 방법
         * 따로 로그 볼 필요 없다.
         */
        consumer.subscribe(List.of(TOPIC_NAME));
        Set<TopicPartition> assignedTopicPartition = consumer.assignment();

        /**
        * 컨슈머는 poll() 메서드를 호출하여 데이터를 가져와서 처리한다.
        * 지속적으로 반복 호출 하기 때문에 무한루프 만들어줌
        */
        while (true) {
            /*poll() 메서드는 Duration 타입의 인자를 받는데 이 인자 값은 브로커로부터 데이터를 가져올때
            컨슈머 버퍼에 데이터를 기다리기위한 타임아웃 간격을 뜻한다.*/
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records){
                log.info("레코드 : {}",record);
            }

        }

        /**
         * consumer sync-offset-commit
         * 동기, 비동기 방식
         */
//        while (true) {
//
//            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
//            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
//            for (ConsumerRecord<String, String> record : records){
//
//                log.info("레코드 : {}",record);
//
//                currentOffset.put(
//                        new TopicPartition(record.topic(), record.partition()),
//                        new OffsetAndMetadata(record.offset() + 1, null)
//                );
////                동기 방식
//                consumer.commitSync(currentOffset);
////                비동기 방식
//                consumer.commitAsync(new OffsetCommitCallback() {
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offset, Exception e) {
//                        if (e != null){
//                            log.error("commit failed",e);
//
//                        } else {
//                            log.info("commit success");
//
//                        }
//                        if (e != null){
//                            log.error("commit failed,offset : {}",offset,e);
//                        }
//                    }
//                });
//
//            }
//        }
//
        /**
         * 리밸런스 리스너
         */
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//            for (ConsumerRecord<String, String> record : records) {
//                currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
//                        new OffsetAndMetadata(record.offset() + 1, null));
//                consumer.commitAsync();
//
//            }
//        }


    }
    private static class RebalanceListener implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.warn("Partitions are assigned");
        }
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("Partitions are revoked");
//            consumer.commitSync(currentOffsets);
        }
    }
}

