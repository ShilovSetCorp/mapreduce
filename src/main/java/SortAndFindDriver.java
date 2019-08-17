import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SortAndFindDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        if (strings.length != 2) {
            System.out.printf("Two parameters are required for SecondarySortBasicDriver- <input dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf(), "Sort");
        job.setJarByClass(SortAndFindDriver.class);
        // Setup MapReduce
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(CompositeValue.class);
        job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(CompositeValue.class);
        job.setPartitionerClass(ActualKeyPartitioner.class);
        job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        AvroKeyInputFormat.addInputPath(job, new Path(strings[0]));
        job.setInputFormatClass(AvroKeyInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        // Execute job
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class Map extends Mapper<AvroKey<GenericData.Record>, NullWritable, CompositeKey, CompositeValue>{
        @Override
        protected void map(AvroKey<GenericData.Record> key, NullWritable  value, Context context) throws IOException, InterruptedException {
            long bookingId = (long) key.datum().get("id");
            long hotelId = (long) key.datum().get("hotel_id");
            String srchCi = (String) key.datum().get("srch_ci");
            CompositeKey ck = new CompositeKey();
            ck.setHotelId(hotelId);
            ck.setSrchCi(srchCi);
            ck.setBookingId(bookingId);
            int channel = (int) key.datum().get("channel");
            int adults = (int) key.datum().get("srch_adults_cnt");
            CompositeValue cv = new CompositeValue(channel, adults);
            context.write(ck, cv);
        }
    }

    public static class Reduce extends Reducer<CompositeKey, CompositeValue, Text, Text> {
        @Override
        protected void reduce(CompositeKey key, Iterable<CompositeValue> values, Context context) throws IOException, InterruptedException {
            Text text = new Text("Hotel ID: " + key.getHotelId() + ", Srch_ci: " + key.getSrchCi() + "Booking id: " + key.getBookingId());
            List<CompositeValue> cvWithCompanies = StreamSupport.stream(values.spliterator(), false)
                    .filter(compositeValue -> compositeValue.getAdults() >= 2)
                    .collect(Collectors.toList());
            context.write(text, new Text("" + cvWithCompanies.get(cvWithCompanies.size()-1).getChannel()));
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SortAndFindDriver(), args);
        System.exit(exitCode);
    }

}
