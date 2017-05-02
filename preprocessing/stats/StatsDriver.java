package preprocessing.stats;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * outputs (movieid, stats) tuple for each movie
 * stats consists of numSI and sumSI
 */

public class StatsDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new Configuration(), new StatsDriver(), args);
		 System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.out.printf("Usage: StatsDriver <input dir> <output dir>\n");
		      return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(StatsDriver.class);
	    job.setJobName("Compute numSI and sumSI for each item");
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(StatsMapper.class);
	    job.setReducerClass(StatsReducer.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(DoubleWritable.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(StatsWritable.class);

	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
}
