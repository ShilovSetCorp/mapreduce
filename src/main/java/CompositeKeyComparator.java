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
        //if hotel ids are the same should try to sort by srch_ci
        if (compare == 0) {
            compare = key1.getSrchCi().compareTo(key2.getSrchCi());
            //if we srch_ci are equal should try to sort by booking ids
            if(compare == 0){
                compare = key1.getBookingId() > key2.getBookingId() ? 1 : -1;
                if(key1.getHotelId() == key2.getHotelId()){
                    compare = 0;
                }
            }
        }
        return compare;
    }
}
