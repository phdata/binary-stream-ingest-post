package phdata;

public class BinaryStreamConfigConstants {
    /**
     * URL endpoint to stream audio
     */
    public static final String CONFIG_URL = "url";
    /**
     * Interval is not used
     */
    public static final String CONFIG_INTERVAL = "interval";
    /**
     * Amount of data per event
     */
    public static final String CONFIG_EVENT_SIZE = "event_size";
    /**
     * Topic for Kafka
     */
    public static final String CONFIG_TOPIC = "topic";
    /**
     * Kinesis property file
     */
    public static final String CONFIG_KINESIS_PROP_FILE = "/kinesis/binary-kinesis.properties";
    /**
     * Kafka property file
     */
    public static final String CONFIG_KAFKA_PROP_FILE = "/kafka/binary-producer.properties";
}
