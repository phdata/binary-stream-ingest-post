package phdata.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import phdata.BinaryReader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static phdata.BinaryStreamConfigConstants.*;

public class BinaryStreamSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger LOG = LoggerFactory.getLogger(BinaryStreamSource.class);
    private final List<Event> eventList = new ArrayList<Event>();
    private String url;
    private long interval;
    private int eventSize;
    private BinaryReader bReader;

    @Override
    public Status process() throws EventDeliveryException {
        Event event;
        Map<String, String> headers;
        long processStart = System.currentTimeMillis();
        try {
            while (((processStart + this.interval) > System.currentTimeMillis())) {
                long readStart = System.currentTimeMillis();
                byte[] rtn = bReader.read();

                headers = new HashMap<String, String>();
                headers.put("start", String.valueOf(readStart));
                headers.put("end", String.valueOf(System.currentTimeMillis()));

                event = EventBuilder.withBody(rtn, headers);
                eventList.add(event);
            }
        } catch (IOException ex) {
            throw new EventDeliveryException(ex.getMessage());
        }
        // process events
        ChannelException ex = null;
        try {
            getChannelProcessor().processEventBatch(eventList);
        } catch (ChannelException chEx) {
            ex = chEx;
        }
        eventList.clear();
        return Status.READY;
    }

    @Override
    public void configure(Context context) {
        this.url = context.getString(CONFIG_URL);
        this.interval = context.getLong(CONFIG_INTERVAL, 5000L);
        this.eventSize = context.getInteger(CONFIG_EVENT_SIZE, 10000);
        LOG.info("url = " + url);
        LOG.info("interval = " + this.interval);
        LOG.info("eventSize = " + this.eventSize);

    }

    @Override
    public synchronized void stop() {
        LOG.debug("Stop");
        try {
            bReader.close();
        } catch (Exception ex) {
            LOG.error("Error stopping source and cleaning up input stream");
            throw new FlumeException(ex);
        }
        super.stop();
    }

    @Override
    public synchronized void start() {
        LOG.debug("Start");
        try {
            URL url = new URL(this.url);
            URLConnection connection = url.openConnection();
            InputStream inStream = connection.getInputStream();
            bReader = new BinaryReader(inStream, this.eventSize);
        } catch (Exception ex) {
            LOG.error("Unable to open connection to " + this.url);
            throw new FlumeException(ex);
        }
        super.start();
    }
}
