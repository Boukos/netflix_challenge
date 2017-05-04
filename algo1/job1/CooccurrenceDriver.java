package algo1.job1;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CooccurrenceDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new Configuration(), new CooccurrenceDriver(), args);
		 System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.out.printf("Usage: CooccurenceDriver <input dir> <output dir>\n");
		      return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(CooccurrenceDriver.class);
	    job.setJobName("Compute co-occurrence of movies");
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(CooccurrenceMapper.class);
	    
	    job.setPartitionerClass(IDPartitioner.class);
	    
	    job.setGroupingComparatorClass(IDGroupingComparator.class);
	    job.setSortComparatorClass(IDSortComparator.class);
	    
	    job.setOutputKeyClass(IDPairWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);

	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
}
