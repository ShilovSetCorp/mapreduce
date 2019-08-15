import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    private long hotelId;
    private String srchCi;
    private long bookingId;

    long getBookingId() {
        return bookingId;
    }

    void setBookingId(long bookingId) {
        this.bookingId = bookingId;
    }

    long getHotelId() {
        return hotelId;
    }

    void setHotelId(long hotelId) {
        this.hotelId = hotelId;
    }

    String getSrchCi() {
        return srchCi;
    }

    void setSrchCi(String srchCi) {
        this.srchCi = srchCi;
    }

    CompositeKey() {
    }

    public int compareTo(CompositeKey o) {
        int result = hotelId > o.getHotelId() ? 1 : -1;
        if (hotelId == o.getHotelId()){
            result = srchCi.compareTo(o.getSrchCi());
            if(result == 0){
                result = bookingId > o.getBookingId() ? 1 : -1;
                if (bookingId == o.getBookingId()){
                    result = 0;
                }
            }
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVLong(dataOutput, hotelId);
        WritableUtils.writeString(dataOutput, srchCi);
        WritableUtils.writeVLong(dataOutput, bookingId);
    }

    public void readFields(DataInput dataInput) throws IOException {
        hotelId = WritableUtils.readVLong(dataInput);
        srchCi = WritableUtils.readString(dataInput);
        bookingId = WritableUtils.readVLong(dataInput);
    }
}
