import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import org.apache.hadoop.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.hadoop.compression.lzo.*;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapred.lib.*;




public class RecompressSingle extends Configured implements Tool{
	
	public static void main(String[] args)throws Exception
	{
		int ecode = ToolRunner.run(new RecompressSingle(), args);
		System.exit(ecode);
	}
	public int run (String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "datacompression");
		 
		job.setJarByClass(RecompressSingle.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(1);
		
		FileOutputFormat.setCompressOutput(job,true);
		FileOutputFormat.setOutputCompressorClass(job,LzopCodec.class);
		
		return(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
