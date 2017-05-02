package preprocessing.si;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SIDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new Configuration(), new SIDriver(), args);
		 System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.out.printf("Usage: SIDriver <input dir> <output dir>\n");
		      return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(SIDriver.class);
	    job.setJobName("Specific Interaction Calculation");
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(SIMapper.class);
	    job.setNumReduceTasks(0);
	    
	    job.setOutputKeyClass(IDPairWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);

	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
}
