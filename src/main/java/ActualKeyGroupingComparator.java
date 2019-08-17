import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * ActualKeyGroupingComparator extends WritableComparator
 * It helps to make sure that grouping will be only by hotel_id, not whole CompositeKey
 */
public class ActualKeyGroupingComparator extends WritableComparator {
    protected ActualKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        if(key1.getHotelId() == key2.getHotelId()){
            return 0;
        }
        return key1.getHotelId() > key2.getHotelId() ? 1 : -1;
    }
}
