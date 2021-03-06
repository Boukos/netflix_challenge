package algo1.job2;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Similarity Driver -- computes pearson correlation given cooccurences
 *  Input: ((item_id, item_id), (stats1, stats2))
 *  Output ((item_id, item_id), similarity)
 */

public class SimilarityDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new Configuration(), new SimilarityDriver(), args);
		 System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.out.printf("Usage: SimilarityDriver <input dir> <output dir>\n");
		      return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(SimilarityDriver.class);
	    job.setJobName("Similarity Driver");
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(SimilarityMapper.class);
	    job.setReducerClass(SimilarityReducer.class);
	    
	    job.setMapOutputKeyClass(IDPairWritable.class);
	    job.setMapOutputValueClass(StatsPairWritable.class);
	    
	    job.setOutputKeyClass(IDPairWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);

	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
}
