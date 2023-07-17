import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.StringTokenizer;

public class KafkaConsumerConfig {
    private static final Logger log = LogManager.getLogger(KafkaConsumerConfig.class);
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final String autoOffsetReset = "latest";
    private final String enableAutoCommit = "false";
    private final String sleep;
    private final String additionalConfig;

    public KafkaConsumerConfig(String bootstrapServers, String topic, String groupId,
                             String sleep,
                               String additionalConfig) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.sleep = sleep;
        this.additionalConfig = additionalConfig;
    }
    public static KafkaConsumerConfig fromEnv() {
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String topic = System.getenv("TOPIC");
        String sleep = System.getenv("SLEEP");

        String groupId = System.getenv("GROUP_ID");
        String additionalConfig = System.getenv()
                .getOrDefault("ADDITIONAL_CONFIG", "");

        return new KafkaConsumerConfig(bootstrapServers, topic, groupId,
                 sleep, additionalConfig);
    }

    public static Properties createProperties(KafkaConsumerConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                CustomerDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                "200");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                "500");

        if (!config.getAdditionalConfig().isEmpty()) {
            StringTokenizer tok = new StringTokenizer(config.getAdditionalConfig(), ", \t\n\r");
            while (tok.hasMoreTokens()) {
                String record = tok.nextToken();
                int endIndex = record.indexOf('=');
                if (endIndex == -1) {
                    throw new RuntimeException("Failed to parse Map from String");
                }
                String key = record.substring(0, endIndex);
                String value = record.substring(endIndex + 1);
                props.put(key.trim(), value.trim());
            }
        }
        return props;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }
    public String getTopic() {
        return topic;
    }
    public String getGroupId() {
        return groupId;
    }
    public String getSleep() {
        return sleep;
    }



    public String getAdditionalConfig() {
        return additionalConfig;
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig{" +
            "bootstrapServers='" + bootstrapServers + '\'' +
            ", topic='" + topic + '\'' +
            ", groupId='" + groupId + '\'' +
            ", autoOffsetReset='" + autoOffsetReset + '\'' +
            ", enableAutoCommit='" + enableAutoCommit + '\'' +
            ", additionalConfig='" + additionalConfig + '\'' +
            '}';
    }
}
