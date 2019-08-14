import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }


    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {

        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;

// (first check on hotel id)
        int compare = key1.getHotelId() > key2.getHotelId() ? 1 : -1;
        if(key1.getHotelId() == key2.getHotelId()){
            compare = 0;
        }

        if (compare == 0) {
// only if we are in the same input group should we try and sort by value
            return key1.getSrchCi().compareTo(key2.getSrchCi());
        }
        return compare;
    }
}
