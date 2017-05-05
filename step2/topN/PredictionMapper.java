package step2.topN;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PredictionMapper extends Mapper<LongWritable, Text, LongWritable, IDSIWritable> {

	
	// the position of the key in the line
	private static int MOVIE_ID = 0;
	
	
	// position of rating in the line
	private static int USER_ID = 1;
	private static int SI_INDEX = 0;
	
	private final LongWritable keyOut = new LongWritable();
	private final IDSIWritable valOut = new IDSIWritable();

	
	/*
	 * emits either movieid or userid for the key, and the rating for the value
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		
		String[] kv = record.split("\t");
		
		String[] keys = kv[0].split(",");
		
		int user_id = Integer.parseInt(keys[USER_ID]);
		int movie_id = Integer.parseInt(keys[MOVIE_ID]);
		double si = Double.parseDouble(kv[SI_INDEX]);
		
		
		keyOut.set(user_id);
		valOut.setmovieID(movie_id);
		valOut.setSI(si);
		
		context.write(keyOut, valOut);
	}
}
