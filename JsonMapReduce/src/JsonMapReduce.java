import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.slf4j.*;

import java.io.IOException;

public final class JsonMapReduce {
  private static final Logger log = LoggerFactory.getLogger(JsonMapReduce.class);

  public static class Map extends Mapper<LongWritable, MapWritable, NullWritable, Text> {

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        context.write(NullWritable.get(), value);
    }
  }

  public static void main(String... args) throws Exception { 
	  runJob(args[0], args[1]);
  }

  public static void runJob(String input, String output) throws Exception {
	  
    Configuration conf = new Configuration();
    Job job = new Job(conf);
    job.setJarByClass(JsonMapReduce.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setInputFormatClass(JsonMapReduceInputFormat.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(input));
    Path outPath = new Path(output);
    FileOutputFormat.setOutputPath(job, outPath);
    outPath.getFileSystem(conf).delete(outPath, true);

    job.waitForCompletion(true);
  }
}