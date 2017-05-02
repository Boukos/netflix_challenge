package preprocessing.si;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the SumReducer class from the word count exercise.
 */ 
public class AvgReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

  @Override
  public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
    long count = 0;
    double sum = 0;
    for (DoubleWritable value : values) {
      sum += value.get();
      count++;
    }
    context.write(key, new DoubleWritable(sum/count));
  }
}