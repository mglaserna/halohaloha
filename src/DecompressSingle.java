
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import com.hadoop.compression.lzo.LzopCodec;

public class DecompressSingle extends Configured implements Tool{
	
	public static void main(String[] args)throws Exception
	{
		int ecode = ToolRunner.run(new DecompressSingle(), args);
		
		System.exit(ecode);
	}
	public int run (String[] args) throws Exception
	{
		///////////////////////
		Configuration conf = new Configuration();
		Job job = new Job(conf, "decompressor");
		////////////////////////////////////
		job.setJarByClass(DecompressSingle.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
			
		/////////////////////////////////
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputCompressorClass(job,LzopCodec.class);
		
		FileOutputFormat.setCompressOutput(job,true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
}
