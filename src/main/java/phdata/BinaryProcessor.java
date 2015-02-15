package phdata;

/**
 * Interface used by te URLBinarySteamHandler to delegate processing
 */
public interface BinaryProcessor {
    public void processData(byte[] data, long start, long end);
}