import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class ActualKeyPartitioner extends Partitioner<CompositeKey, Text> {

    HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<>();
    Text newKey = new Text();
    public int getPartition(CompositeKey compositeKey, Text text, int i) {
        newKey.set(String.valueOf(compositeKey.getHotelId()));

        return hashPartitioner.getPartition(newKey, text, i);
    }
}
