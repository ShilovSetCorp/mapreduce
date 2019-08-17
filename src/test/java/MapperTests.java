import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class MapperTests {
    private SortAndFindDriver.Map mapper;
    private Context context;
    private Counter counter;
    private AvroKey avroKeyMock;
    private GenericData.Record gdr;

    @Before
    public void init() {
        mapper = new SortAndFindDriver.Map();
        context = mock(Context.class);
        avroKeyMock = mock(AvroKey.class);
        gdr = mock(GenericData.Record.class);
    }

    @Test
    public void testAvroMap() throws IOException, InterruptedException {
        when(gdr.get("hotel_id")).thenReturn(100L);
        when(gdr.get("id")).thenReturn(10L);
        when(gdr.get("srch_ci")).thenReturn("mock");
        when(gdr.get("channel")).thenReturn(3);
        when(gdr.get("srch_adults_cnt")).thenReturn(4);
        when(avroKeyMock.datum()).thenReturn(gdr);

        mapper.map(avroKeyMock, null, context);
        verify(gdr, times(1)).get("id");
        verify(gdr, times(1)).get("srch_ci");
        verify(gdr, times(1)).get("hotel_id");
        verify(gdr, times(1)).get("channel");
        verify(gdr, times(1)).get("srch_adults_cnt");
    }

}
