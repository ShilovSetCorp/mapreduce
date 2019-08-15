import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class MapperTests {
    private SortAndFindDriver.Map mapper;
    private Context context;
    private Counter counter;
    private AvroKey avroKeyMock;
    private Object datumMock;
    @Before
    public void init(){
        mapper = new SortAndFindDriver.Map();
        context = mock(Context.class);
        counter = mock(Counter.class);
        avroKeyMock = mock(AvroKey.class);

        doNothing().when(counter).increment(anyLong());
    }

    @Test
    public void testAvroMap() throws IOException, InterruptedException {

        mapper.map(avroKeyMock, null, context);

        verify(counter, times(1)).increment(1);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(context);
    }
}
