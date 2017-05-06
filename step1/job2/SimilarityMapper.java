package step1.job2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import step1.job1.IDPairWritable;
import step1.job1.StatsWritable;

public class SimilarityMapper extends Mapper<LongWritable, Text, IDPairWritable, StatsPairWritable> {
	
	
	private final IDPairWritable keyOut = new IDPairWritable();
	private final StatsPairWritable valOut = new StatsPairWritable();
	
	private final StatsWritable stats1 = new StatsWritable();
	private final StatsWritable stats2 = new StatsWritable();
	/*
	 * Reads in the file and emits a properly formatted K-V pair with K = Item Pair and V = Stats pair
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		
		String[] vals = record.split(",");
		if (vals.length == 8) {
			keyOut.setMovieID1(Integer.parseInt(vals[0]));
			keyOut.setMovieID2(Integer.parseInt(vals[1]));
		
			
			stats1.setSI(Double.parseDouble(vals[2]));
			stats1.setSumSI(Double.parseDouble(vals[3]));
			stats1.setNumSI(Integer.parseInt(vals[4]));
			
			stats2.setSI(Double.parseDouble(vals[5]));
			stats2.setSumSI(Double.parseDouble(vals[6]));
			stats2.setNumSI(Integer.parseInt(vals[7]));
			
			
			valOut.setStats1(stats1);
			valOut.setStats2(stats2);
			
			context.write(keyOut, valOut);
		}
	}
}
