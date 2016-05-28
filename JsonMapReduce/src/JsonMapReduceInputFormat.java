
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class JsonMapReduceInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text>
    createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new JsonMapReduceReader();
    }


    @Override
    protected boolean isSplitable(JobContext context, Path file) {
    	return true;
    }

}