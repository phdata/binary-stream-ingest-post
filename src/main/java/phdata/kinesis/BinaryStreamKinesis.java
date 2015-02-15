package phdata.kinesis;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.kinesis.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import phdata.BinaryProcessor;
import phdata.URLBinaryStreamHandler;

import static phdata.BinaryStreamConfigConstants.*;

/**
 * Modeled after the AWS Kinesis example
 * https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonKinesis/AmazonKinesisSample.java
 */
public class BinaryStreamKinesis implements BinaryProcessor {

    private static final Log LOG = LogFactory.getLog(BinaryStreamKinesis.class);
    static AmazonKinesisClient kinesisClient;

    private static String streamName;
    private static String urlProp;

    /**
     * initialize kinesis client
     * @throws Exception
     */
    private static void init() throws Exception {

        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        kinesisClient = new AmazonKinesisClient(credentials);
    }

    public static void main(String[] args) throws Exception {
        init();

        streamName = "BinaryStream";
        final Integer streamSize = 1;

        boolean streamExists = true;
        try {
            kinesisClient.describeStream(streamName);
        } catch (ResourceNotFoundException notFound) {
            LOG.info("Stream " + streamName + " does not exist.");
            streamExists = false;
        }

        if (!streamExists) {
            // Create a stream. The number of shards determines the provisioned throughput.
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(streamName);
            createStreamRequest.setShardCount(streamSize);

            kinesisClient.createStream(createStreamRequest);
            // The stream is now being created.
            LOG.info("Creating Stream : " + streamName);
        }

        waitForStreamToBecomeAvailable(streamName);

        LOG.info("Putting records in stream : " + streamName);

        BinaryStreamKinesis binaryStreamKinesis = new BinaryStreamKinesis();
        Properties properties = binaryStreamKinesis.getProperties();
        urlProp = properties.getProperty(CONFIG_URL);
        int eventSize = Integer.valueOf(properties.getProperty(CONFIG_EVENT_SIZE, "5000")).intValue();
        long interval = Long.valueOf(properties.getProperty(CONFIG_INTERVAL, "10000")).longValue();
        AtomicBoolean stop = new AtomicBoolean(false);
        URLBinaryStreamHandler urlBinaryStreamHandler = new URLBinaryStreamHandler(urlProp, interval, eventSize, stop, binaryStreamKinesis);
        Thread urlThread = new Thread(urlBinaryStreamHandler);
        urlThread.start();

    }

    private static void waitForStreamToBecomeAvailable(String streamName) {

        System.out.println("Waiting for " + streamName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000 * 20);
            } catch (InterruptedException e) {
                // Ignore interruption (doesn't impact stream creation)
            }
            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(streamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.println("  - current state: " + streamStatus);
                if (streamStatus.equals("ACTIVE")) {
                    return;
                }
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) {
                    throw ase;
                }
                throw new RuntimeException("Stream " + streamName + " never went active");
            }
        }
    }

    public Properties getProperties() throws IOException {
        Properties props = new Properties();
        InputStream in = getClass().getResourceAsStream(CONFIG_KINESIS_PROP_FILE);
        props.load(in);
        in.close();
        return props;
    }

    @Override
    public void processData(byte[] data, long start, long end) {
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setData(ByteBuffer.wrap(data));
        putRecordRequest.setPartitionKey(urlProp);
        PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
    }
}
