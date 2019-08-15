import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class ReducerTests {
    private SortAndFindDriver.Reduce reducer;
    private Reducer.Context context;
    private Counter counter;

    @Before
    public void init(){
        reducer = new SortAndFindDriver.Reduce();
        context = mock(Reducer.Context.class);
        counter = mock(Counter.class);
        doNothing().when(counter).increment(anyLong());
    }

    @Test
    public void testAvroMap() throws IOException, InterruptedException {
        List<Text> iterables = new ArrayList<>();
        iterables.add(new Text());
        iterables.add(new Text());
        iterables.add(new Text());
        reducer.reduce(new CompositeKey(), iterables, context);
        verify(counter, times(1)).increment(1);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(context);
    }
}
