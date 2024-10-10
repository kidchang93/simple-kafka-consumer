package kr.co.simplekafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootApplication
public class KstreamKtableJoin {

    private final static String APPLICATION_NAME = "order-join-application";
    private final static String STREAM_LOG = "stream_log";
    private final static String BOOTSTRAP_SERVERS = "172.31.102.28:9092";
    private final static String STREAM_LOG_FILTER = "stream_log_filter";
    private final static String ADDRESS_TABLE = "address";
    private final static String ORDER_STREAM = "order";
    private final static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SpringApplication.run(KstreamKtableJoin.class, args);

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // 카프카 스트림과 테이블을 정의해준다.
        KTable<String, String> addressTable = builder.table(ADDRESS_TABLE);
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        orderStream.join(addressTable, (order, address) -> order + " send to " + address).to(ORDER_JOIN_STREAM);

        // 정의한 토폴로지에 대한 정보와 스트림즈 실행을 위한 기본 옵션을 파라미터로 인스턴스 생성.
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // 스타트
        streams.start();
    }
}
