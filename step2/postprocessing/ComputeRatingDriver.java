package step2.postprocessing;

import algo1.job1.IDPairWritable;
import algo1.job2.SimilarityMapper;
import algo1.job2.SimilarityReducer;
import algo1.job2.StatsPairWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ComputeRatingDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new Configuration(), new ComputeRatingDriver(), args);
		 System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.out.printf("Usage: PredictionDriver <input dir> <output dir>\n");
		      return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(ComputeRatingDriver.class);
	    job.setJobName("Get rating from SI");
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(RatingMapper.class);
	    job.setNumReduceTasks(0);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
}
