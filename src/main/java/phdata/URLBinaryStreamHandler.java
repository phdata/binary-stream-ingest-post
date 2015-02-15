package phdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Common URLHandler used for each implementation
 * Calls BinaryProcessor
 */
public class URLBinaryStreamHandler implements Runnable {
    Logger LOG = LoggerFactory.getLogger(URLBinaryStreamHandler.class);

    private String url;
    private long interval;
    private int eventSize;
    private InputStream inputStream;
    private BinaryReader binaryReader;
    private AtomicBoolean stop;
    private BinaryProcessor processor;

    public URLBinaryStreamHandler(String url, long interval, int eventSize,
                                  AtomicBoolean stop, BinaryProcessor processor) {
        this.url = url;
        this.interval = interval;
        this.eventSize = eventSize;
        this.stop = stop;
        this.processor = processor;
    }

    @Override
    public void run() {
        LOG.info("Starting URL handler");
        try {
            URL url = new URL(this.url);
            URLConnection connection = url.openConnection();
            inputStream = connection.getInputStream();
            binaryReader = new BinaryReader(inputStream, eventSize);
            processEvents();
        } catch (Exception ex) {
            LOG.error("Unable to open connection to " + this.url);
            LOG.error(ex.getMessage());
        }

    }

    public void processEvents() {
        try {
            while (!stop.get()) {
                long start = System.currentTimeMillis();
                byte[] rtn = binaryReader.read();
                long end = System.currentTimeMillis();
                processor.processData(rtn, start, end);
            }
            binaryReader.close();
        } catch (ClosedByInterruptException e) {
            // parent closing
        } catch (IOException ex) {
            ex.printStackTrace();
            LOG.error(ex.getMessage());
        }
    }
}
