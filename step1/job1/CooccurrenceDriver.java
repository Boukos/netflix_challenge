package step1.job1;

import java.net.URI;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Cooccurrence Driver -- computes coocurence for movies (uses side data in mapper)
 *  Input:  (user_id, item_id, stats) 
 *  Output ((item_id, item_id), (stats1, stats2))
 */

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
		
		// separate key/value with comma instead of default tab in output file
		Configuration conf = getConf();
		conf.set("mapred.textoutputformat.separator", ",");
		
		Job job = new Job(conf);
		job.setJarByClass(CooccurrenceDriver.class);
	    job.setJobName("Compute co-occurrence of movies");

	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(CooccurrenceMapper.class);
	    job.setReducerClass(CooccurrenceReducer.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IDStatsWritable.class);
	    
	    job.setOutputKeyClass(IDPairWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/stats.txt#stats.txt"), job.getConfiguration());

	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
}