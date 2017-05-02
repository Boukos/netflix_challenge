package preprocessing.stats;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatsMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		String[] vals = record.split("\t");
		if (vals.length == 2) {
			String[] ids = vals[0].split(",");
			if (ids.length == 2) {
				int movieid = Integer.parseInt(ids[0]);
				double si = Double.parseDouble(vals[1]);
				context.write(new IntWritable(movieid), new DoubleWritable(si));
			}
		}
	}
	
}
