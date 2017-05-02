package preprocessing.stats;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the SumReducer class from the word count exercise.
 */ 
public class StatsReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, StatsWritable> {

  @Override
  public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
	  int count = 0;
	  double sum = 0;
	  for (DoubleWritable val: values) {
		  sum += val.get();
		  count++;
	  }
	  context.write(key, new StatsWritable(count, sum));
  }
}