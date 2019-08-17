import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/*
 * ActualKeyPartitioner extends Partitioner class, makes partitions only by hotel_id field,
 * not by whole CompositeKey class
 */
public class ActualKeyPartitioner extends Partitioner<CompositeKey, Text> {

    private HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<>();
    private Text newKey = new Text();
    public int getPartition(CompositeKey compositeKey, Text text, int i) {
        newKey.set(String.valueOf(compositeKey.getHotelId()));
        return hashPartitioner.getPartition(newKey, text, i);
    }
}
