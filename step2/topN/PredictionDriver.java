package step2.topN;

import java.net.URI;

import algo1.job1.IDPairWritable;
import algo1.job2.SimilarityMapper;
import algo1.job2.SimilarityReducer;
import algo1.job2.StatsPairWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PredictionDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new Configuration(), new PredictionDriver(), args);
		 System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.out.printf("Usage: PredictionDriver <input dir> <output dir>\n");
		      return -1;
		}
		
	    Configuration conf = getConf();
		conf.set("mapred.textoutputformat.separator", ",");
		
		Job job = new Job(conf);
		job.setJarByClass(PredictionDriver.class);
	    job.setJobName("Prediction Driver");
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(PredictionMapper.class);
	    job.setReducerClass(PredictionReducer.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/finalfather/part-r-00000#part-r-00000"), job.getConfiguration());
	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/finalfather/part-r-00001#part-r-00001"), job.getConfiguration());
	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/finalfather/part-r-00002#part-r-00002"), job.getConfiguration());
	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/finalfather/part-r-00003#part-r-00003"), job.getConfiguration());
	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/finalfather/part-r-00004#part-r-00004"), job.getConfiguration());
	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/finalfather/part-r-00005#part-r-00005"), job.getConfiguration());
	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/finalfather/part-r-00006#part-r-00006"), job.getConfiguration());

	    DistributedCache.addCacheFile(new URI("s3n://netflix-final-project-sp17/TestingRatings.txt#TestingRatings.txt"), job.getConfiguration());

	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
}
