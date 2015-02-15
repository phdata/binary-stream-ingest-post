package phdata;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Common reader class for consuming a byte stream
 * and ensuring a specific amount of data is read before returning
 */
public class BinaryReader {

    private InputStream in;
    private ByteBuffer bBuf;
    private ReadableByteChannel channel;

    public BinaryReader(InputStream in, int bufferSize) {
        this.in = in;
        this.channel = Channels.newChannel(in);
        this.bBuf = ByteBuffer.allocate(bufferSize);
    }

    public byte[] read() throws IOException{
        int bytesRead = 0;
        while (bBuf.hasRemaining()){
            bytesRead += channel.read(bBuf);
        }
        bBuf.rewind();
        byte[] rtn = new byte[bytesRead];
        bBuf.get(rtn);
        bBuf.clear();
        return rtn;
    }

    public void close() throws IOException{
        if (channel != null)
            this.channel.close();
        if (this.in != null)
            this.in.close();
    }

}
