import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.json.*;

import java.io.IOException;
import java.io.InputStream;

public class JsonMapReduceReader 
extends RecordReader<LongWritable, Text> {

private long start;
private long pos;
private long end;
private LineReader in;
private Text endChar;
private LongWritable key = new LongWritable();
private Text value = new Text();

private static final Log LOG = LogFactory.getLog(
        JsonMapReduceReader.class);

/**
 * From Design Pattern, O'Reilly...
 * This method takes as arguments the map task’s assigned InputSplit and
 * TaskAttemptContext, and prepares the record reader. For file-based input
 * formats, this is a good place to seek to the byte position in the file to
 * begin reading.
 */
@Override
public void initialize( InputSplit genericSplit,  TaskAttemptContext context) throws IOException {


    FileSplit split = (FileSplit) genericSplit;

    Configuration job = context.getConfiguration();

    // SC: Fix Me: Put these as a configurable value
    this.endChar = new Text("}");

    start = split.getStart();
    end = start + split.getLength();

    final Path file = split.getPath();
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    in = new LineReader(fileIn, job);
    
    this.pos = start;
	LOG.info("JsonMapReduceReader: Initialized!");

}

/**
 * From Design Pattern, O'Reilly...
 * Like the corresponding method of the InputFormat class, this reads a
 * single key/ value pair and returns true until the data is consumed.
 */
@Override
public boolean nextKeyValue() throws IOException {
    Text dummy_str = new Text();
    Text dummy_line = new Text();
	JSONObject obj;
	
    // Current offset is the key
    key = null;
    value.clear();
    
    int newSize = 0;

    // Make sure we get at least one record that starts in this Split
    while (pos < end) {
    	
        newSize = in.readLine(dummy_line);
       
    	LOG.debug("ReadLine: -" + dummy_line + "- with bytes:" + newSize + " at pos " + pos);
        
        if (newSize == 0) {
            break;
        }
        dummy_str.append(dummy_line.getBytes(), 0, dummy_line.getLength());
        
        pos += newSize;

        // IF we read a whole JSON ie: content between { }, then we can parse and return! 
        if (dummy_line.equals( endChar)) {

        	LOG.debug("Found endChar " + newSize + " at pos " + pos);

			try {
				obj = new JSONObject(dummy_str.toString());
				value.set(obj.getString("date") + ',' + obj.getString("type") + ',' + obj.getInt("id") + ',' + obj.getInt("user"));
	        	LOG.debug("Returning:" + value.toString());
			} catch (JSONException e) {
				LOG.warn("Couldn't parse line:" + dummy_str.toString());
			}
        	break;
        }
        
    }

     
    if (newSize == 0) {
        key = null;
        value = null;
        return false;
    } else {
        return true;
    }
}

@Override
public LongWritable getCurrentKey() throws IOException,
        InterruptedException {
    return key;
}

@Override
public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
}

@Override
public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
        return 0.0f;
    } else {
        return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
}

@Override
public void close() throws IOException {
    if (in != null) {
        in.close();
    }
}

}