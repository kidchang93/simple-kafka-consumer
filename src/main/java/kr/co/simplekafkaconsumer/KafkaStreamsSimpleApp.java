package kr.co.simplekafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootApplication
public class KafkaStreamsSimpleApp {

    private final static String APPLICATION_NAME = "streams-application";
    private final static String STREAM_LOG = "stream_log";
    private final static String BOOTSTRAP_SERVERS = "172.31.102.28:9092";
    private final static String STREAM_LOG_COPY = "stream_log_copy";

//    컨슈머의 안전한 종료

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SpringApplication.run(KafkaStreamsSimpleApp.class, args);

        Properties props = new Properties();
        // APPLICATION_ID_CONFIG 을 지정해줘야한다. 아이디 값을 기준으로 병렬처리 하기 때문에
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);

        // 1개 이상의 카프카 브로커 호스트와 포트정보를 입력한다. 연동을 위해
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // 스트림 처리를 위해 직렬화,역직렬화의 방식을 정한다.
        // 스트림즈 애플리케이션에서는 데이터를 처리할 때 메시지 키 또는 값을 역직렬화 해서 사용하고
        // 최종적으로는 데이터를 토픽에 넣을때는 직렬화 해서 데이터를 저장.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // stream() 외에 table(), globalTable() 메서드들은 최초의 토픽 데이터를 가져오는 소스 프로세서이다.
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);

        // stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송하기 위해
        // to() 메서드 사용.
        // to() 메서드는 싱크 프로세서이다.
        streamLog.to(STREAM_LOG_COPY);

        // 정의한 토폴로지에 대한 정보와 스트림즈 실행을 위한 기본 옵션을 파라미터로 인스턴스 생성.
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // 스타트
        streams.start();
    }
}
