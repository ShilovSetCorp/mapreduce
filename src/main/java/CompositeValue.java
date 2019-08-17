import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * CompositeValue extends ObjectWritable.
 * It stores data that need to be passed from mapper to reducer
 */
public class CompositeValue extends ObjectWritable implements WritableComparable<CompositeValue> {
    private Integer channel;
    private Integer adults;

    public CompositeValue() {
    }

    CompositeValue(Integer channel, Integer adults)  {
        this.channel = channel;
        this.adults = adults;
    }

    Integer getChannel() {
        return channel;
    }

    public void setChannel(Integer channel) {
        this.channel = channel;
    }

    Integer getAdults() {
        return adults;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVInt(dataOutput, channel);
        WritableUtils.writeVInt(dataOutput, adults);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        channel = WritableUtils.readVInt(dataInput);
        adults = WritableUtils.readVInt(dataInput);
    }

    @Override
    public int compareTo(CompositeValue o) {
        return 0;
    }
}
