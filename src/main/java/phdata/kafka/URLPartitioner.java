package phdata.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class URLPartitioner implements Partitioner {
    public URLPartitioner (VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        String stringKey = (String) key;
        partition = stringKey.hashCode() % a_numPartitions;
        return partition;
    }

}
