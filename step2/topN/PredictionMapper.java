package step2.topN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import algo1.job1.Pair;

public class PredictionMapper extends Mapper<LongWritable, Text, IntWritable, IDSIWritable> {

	
	// the position of the key in the line
	private static int MOVIE_ID = 0;
	private static int USER_ID = 1;
	
	// position of rating in the line
	private static int SI_INDEX = 1;
	
	private static final String SIDE_DATA = "TestingRatings.txt";
	
	private final IntWritable keyOut = new IntWritable();
	private final IDSIWritable valOut = new IDSIWritable();

	private final Set<Integer> testUserID = new HashSet<Integer>();
		
	@Override
	public void setup(Context context) {
		// load stats into memory
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(SIDE_DATA));
			String currLine;
			while ((currLine = br.readLine()) != null) {
				String[] split = currLine.split(",");
				if (split.length == 3) {
					int userid = Integer.parseInt(split[1]);
					testUserID.add(userid);
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (br != null) br.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
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
		
		if(testUserID.contains(user_id)){
			keyOut.set(user_id);
			valOut.setmovieID(movie_id);
			valOut.setSI(si);
			context.write(keyOut, valOut);
		}
	}
}
