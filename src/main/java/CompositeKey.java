import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    long hotelId;
    String srchCi;

    public long getHotelId() {
        return hotelId;
    }

    public void setHotelId(long hotelId) {
        this.hotelId = hotelId;
    }

    public String getSrchCi() {
        return srchCi;
    }

    public void setSrchCi(String srchCi) {
        this.srchCi = srchCi;
    }

    public CompositeKey() {
    }

    public int compareTo(CompositeKey o) {
        int result = hotelId > o.getHotelId() ? 1 : -1;
        if (hotelId == o.getHotelId()){
            result = srchCi.compareTo(o.getSrchCi());
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVLong(dataOutput, hotelId);
        WritableUtils.writeString(dataOutput, srchCi);
    }

    public void readFields(DataInput dataInput) throws IOException {
        hotelId = WritableUtils.readVLong(dataInput);
        srchCi = WritableUtils.readString(dataInput);
    }
}
