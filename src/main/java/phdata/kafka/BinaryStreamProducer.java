package phdata.kafka;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import phdata.BinaryProcessor;
import phdata.URLBinaryStreamHandler;

import static phdata.BinaryStreamConfigConstants.*;

public class BinaryStreamProducer  implements BinaryProcessor{

    private int eventSize = 0;
    private String urlProp;
    private String topicProp;
    private Producer<String, byte[]> producer;

    public static void main(String[] args) throws Exception {

        BinaryStreamProducer bp = new BinaryStreamProducer();

        // Read properties from file
        Properties props = bp.getProperties();
        bp.urlProp = props.getProperty(CONFIG_URL);
        bp.topicProp = props.getProperty(CONFIG_TOPIC, "binary_topic");
        int eventSize = Integer.valueOf(props.getProperty(CONFIG_EVENT_SIZE, "10000")).intValue();
        long interval = Long.valueOf(props.getProperty(CONFIG_INTERVAL, "10000")).longValue();

        ProducerConfig config = new ProducerConfig(props);

        bp.producer = new Producer<String, byte[]>(config);

        AtomicBoolean stop = new AtomicBoolean(false);

        URLBinaryStreamHandler urlBinaryStreamHandler = new URLBinaryStreamHandler(bp.urlProp, interval, eventSize, stop, bp);

        Thread urlThread = new Thread(urlBinaryStreamHandler);
        urlThread.start();
        urlThread.join();
    }

    public Properties getProperties() throws IOException {
        Properties props = new Properties();
        InputStream in = getClass().getResourceAsStream(CONFIG_KAFKA_PROP_FILE);
        props.load(in);
        in.close();
        return props;
    }

    @Override
    public void processData(byte[] data, long start, long end) {
            KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>(topicProp, urlProp, data);
            producer.send(message);
    }
}
