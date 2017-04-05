import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import com.hadoop.compression.lzo.LzopCodec;


public class recompressingoptimus {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
		"<output path>");
		System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(recompressingoptimus.class);
		job.setNumReduceTasks(1);
        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
 
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.setOutputKeyClass( NullWritable.class );
		//job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
