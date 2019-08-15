import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

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
        job.setNumReduceTasks(2);

        // Specify key / value
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(Text.class);
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

    public static class Map extends Mapper<AvroKey<GenericData.Record>, NullWritable, CompositeKey, Text>{
        @Override
        protected void map(AvroKey<GenericData.Record> key, NullWritable  value, Context context) throws IOException, InterruptedException {
            System.out.println("In the mapper");
            long hotelId = (Long) key.datum().get("hotel_id");
            CharSequence srchCi = (CharSequence) key.datum().get("srch_ci");
            CompositeKey ck = new CompositeKey();
            ck.setHotelId(hotelId);
            ck.setSrchCi(srchCi.toString());
            Integer channel = (Integer) key.datum().get("channel");
            Integer adults = (Integer) key.datum().get("srch_adults_cnt");
            if(adults >= 2) {
                Text text = new Text("channel: " + channel.toString() + ", adults: " + adults);
                System.out.println(text.toString());
                context.write(ck, text);
            }
            System.out.println("Out of mapper");
        }
    }

    public static class Reduce extends Reducer<CompositeKey, Text, Text, Text> {

        @Override
        protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Text text;
            for(Text t : values){
                text = new Text("Hotel id: " + key.getHotelId() + ", srch_ci: " + key.getSrchCi());

                context.write(text, t);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Configuration(), new SortAndFindDriver(), args);
        System.exit(exitCode);
    }

}
