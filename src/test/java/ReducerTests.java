import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class ReducerTests {
    private SortAndFindDriver.Reduce reducer;
    private Reducer.Context context;

    @Before
    public void init(){
        reducer = new SortAndFindDriver.Reduce();
        context = mock(Reducer.Context.class);
    }

    @Test
    public void testAvroReduce() throws IOException, InterruptedException {
        List<CompositeValue> iterables = new ArrayList<>();
        iterables.add(new CompositeValue(1,4));
        iterables.add(new CompositeValue(1,6));
        iterables.add(new CompositeValue(1,2));
        CompositeKey key = new CompositeKey();
        key.setHotelId(123);
        key.setSrchCi("123_234_234");
        Text keyT = new Text("Hotel ID: " + key.getHotelId() + ", Srch_ci: " + key.getSrchCi() + "Booking id: " + key.getBookingId());
        reducer.reduce(key, iterables, context);
        verify(context, times(1)).write(keyT, new Text(iterables.get(iterables.size()-1).getChannel().toString()));
    }

}
