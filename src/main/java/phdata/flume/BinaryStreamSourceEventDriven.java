package phdata.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import phdata.BinaryProcessor;
import phdata.BinaryReader;
import phdata.URLBinaryStreamHandler;

import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static phdata.BinaryStreamConfigConstants.*;

/**
 * Binary Stream Source streams data from URL
 */
public class BinaryStreamSourceEventDriven extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryStreamSourceEventDriven.class);
    private String url = "";
    private long interval = 0L;
    private int eventSize = 0;
    private CounterGroup counterGroup;
    private InputStream inStream;
    private BinaryReader binaryReader;
    private AtomicBoolean urlHandlerThreadStop;
    private Thread urlThread;

    public BinaryStreamSourceEventDriven() {
        this.counterGroup = new CounterGroup();
        this.urlHandlerThreadStop = new AtomicBoolean(false);
    }

    /**
     * Configure source
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        this.url = context.getString(CONFIG_URL);
        this.interval = context.getLong(CONFIG_INTERVAL, 5000L);
        this.eventSize = context.getInteger(CONFIG_EVENT_SIZE, 10000);
        LOG.info("url = " + url);
        LOG.info("interval = " + this.interval);
        LOG.info("eventSize = " + this.eventSize);
    }

    /**
     * Start source which interns starts the URL handler thread
     */
    @Override
    public void start() {
        LOG.info("Binary Source starting");
        try {
            FlumeBinaryProcessor processor = new FlumeBinaryProcessor();
            processor.source = this;

            URLBinaryStreamHandler urlBinaryStreamHandler = new URLBinaryStreamHandler(
                    url, interval, eventSize, urlHandlerThreadStop, processor);

            urlThread = new Thread(urlBinaryStreamHandler);
            urlThread.start();
        } catch (Exception ex) {
            LOG.error("Exception starting thread for url: " + this.url);
            throw new FlumeException(ex);
        }
        super.start();
    }

    /**
     * Stop source interrupt URL Handler to shutdown
     */
    @Override
    public void stop() {
        this.urlHandlerThreadStop.set(true);
        if (urlThread != null) {
            LOG.debug("Stopping url handler thread");

            while (urlThread.isAlive()) {
                try {
                    LOG.debug("Waiting for url handler to finish");
                    urlThread.interrupt();
                    urlThread.join(500);
                } catch (InterruptedException e) {
                    LOG
                            .debug("Interrupted while waiting for accept handler to finish");
                    Thread.currentThread().interrupt();
                }
            }

            LOG.debug("URL handler thread stopped");
        }

        LOG.info("Source stopped. Event metrics:{}", counterGroup);
        super.stop();
    }

    private class FlumeBinaryProcessor implements BinaryProcessor {

        Source source;

        @Override
        public void processData(byte[] data, long start, long end) {
            //Add start and end header timings
            HashMap<String, String> headers = new HashMap<String, String>();
            headers.put("start", String.valueOf(start));
            headers.put("end", String.valueOf(end));

            Event event = EventBuilder.withBody(data, headers);
            // process events
            ChannelException ex = null;
            try {
                this.source.getChannelProcessor().processEvent(event);
            } catch (ChannelException chEx) {
                ex = chEx;
            }

            if (ex == null) {
                counterGroup.incrementAndGet("events.processed");
            } else {
                counterGroup.incrementAndGet("events.failed");
                LOG.warn("Error processing event. Exception: " + ex.getMessage());
            }
        }
    }
}
