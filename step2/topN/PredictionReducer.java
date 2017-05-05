package step2.topN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import algo1.job1.Pair;

/**
 * This is the SumReducer class from the word count exercise.
 */ 
public class PredictionReducer extends Reducer<LongWritable, IDSIWritable, LongWritable, DoubleWritable> {


	private Map<Integer, List<Integer>> testRatings;
	private Map<Integer, Map<Integer, Double>> similarities;
	private static String TEST_DATA = "testMat.txt";
	private static String SIM_DATA = "similarityMat.txt";
	@Override
	public void reduce(LongWritable key, Iterable<IDSIWritable> values, Context context)
			throws IOException, InterruptedException {



		long count = 0;
		double sum = 0;
		for (DoubleWritable value : values) {
			sum += value.get();
			count++;
		}
		context.write(key, new DoubleWritable(sum/count));
	}


	private void getTestData(){
		testRatings = new HashMap<Integer, List<Integer>>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TEST_DATA));
			String currLine;
			while ((currLine = br.readLine()) != null) {
				String[] split = currLine.split(",");
				if (split.length == 3) {
					int movieid = Integer.parseInt(split[0]);
					int userid = Integer.parseInt(split[1]);
					if (testRatings.containsKey(movieid)){
						testRatings.get(movieid).add(userid);
					} else {
						testRatings.put(movieid, new ArrayList<Integer>());
						testRatings.get(movieid).add(userid);
					}
					
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
	
	private void getSimilarityData(){
		similarities = new HashMap<Integer, Map<Integer, Double>>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(SIM_DATA));
			String currLine;
			while ((currLine = br.readLine()) != null) {
				String[] split = currLine.split("\t");
				if (split.length == 2) {
					String[] ids = split[0].split(",");
					int movie1 = Integer.parseInt(ids[0]);
					int movie2 = Integer.parseInt(ids[1]);
					double sim = Double.parseDouble(split[1]);
					if (similarities.containsKey(movie1)){
						similarities.get(movie1).put(movie2, sim);
					} else {
						similarities.put(movie1, new HashMap<Integer, Double>());
						similarities.get(movie1).put(movie2, sim);
					}
					
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
}