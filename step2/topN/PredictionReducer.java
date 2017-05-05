package step2.topN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import algo1.job1.IDPairWritable;
import algo1.job1.Pair;

/**
 * Prediction Reducer
 */ 
public class PredictionReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {

	private static int N = 50;
	private static String TEST_DATA = "TestingRatings.txt";
	private static String [] SIM_DATA = {"part-r-00000","part-r-00001","part-r-00002","part-r-00003","part-r-00004","part-r-00005","part-r-00006"};
	
	
	private Comparator<Pair<Integer, Double>> comp = new Comparator<Pair<Integer, Double>>(){

		@Override
		public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
			return (int)Math.round(o1.getSecond()*100 - o2.getSecond()*100);
		}
		
	};
	private PriorityQueue<Pair<Integer,Double>> simHeap = new PriorityQueue<Pair<Integer,Double>>(N, comp);
	private Map<Integer, List<Integer>> testRatings = new HashMap<Integer, List<Integer>>();
	private Map<Integer, List<Pair<Integer, Double>>> similarities  = new HashMap<Integer, List<Pair<Integer, Double>>>();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
      getTestData();
      getSimilarityData();
    }
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		if(testRatings.get(key.get())!=null){
			List<Integer> targets = testRatings.get(key.get());
			for (Integer movieID: targets){
				simHeap.clear();
				double num = 0;
				double denom = 0;
				if(similarities.get(movieID)==null){
					System.out.println("similarities is null");
					context.write(new Text(key.get() + "," + movieID), new DoubleWritable(0));
				}else{
					for(Pair<Integer, Double> simPair: similarities.get(movieID)){
						simHeap.add(simPair);
						if (simHeap.size() > N){
							simHeap.remove();
						}
					}

					for (Text value: values){
						String [] splitData = value.toString().split(",");
						int movID = Integer.parseInt(splitData[0]);
						double si = Double.parseDouble(splitData[1]);
						for (Pair<Integer, Double> simPair: simHeap){
							if (movID == simPair.getFirst()){ //user has rated this similar movie
								num += simPair.getSecond() * si; //add weighted index params
								denom += simPair.getSecond();
							}
						}
					}
					if(denom==0){
						num=0;
						denom=1;
					}
					context.write(new Text(key.get() + "," + movieID), new DoubleWritable(num/denom));
				}
			}
		}
	}


	private void getTestData(){
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TEST_DATA));
			String currLine;
			while ((currLine = br.readLine()) != null) {
				String[] split = currLine.split(",");
				if (split.length == 3) {
					int movieid = Integer.parseInt(split[0]);
					int userid = Integer.parseInt(split[1]);
					if (testRatings.containsKey(userid)){
						testRatings.get(userid).add(movieid);
					} else {
						testRatings.put(userid, new ArrayList<Integer>());
						testRatings.get(userid).add(movieid);
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
		for(int x=0; x<SIM_DATA.length; x++){
			String filename = SIM_DATA[x];
			BufferedReader br = null;
			try {
				System.out.println("reading filename");
				br = new BufferedReader(new FileReader(filename));
				String currLine;
				int count = 0;
				while ((currLine = br.readLine()) != null) {
					count++;
					String[] split = currLine.split("\t");
					if (split.length == 2) {
						String[] ids = split[0].split(",");
						int movie1 = Integer.parseInt(ids[0]);
						int movie2 = Integer.parseInt(ids[1]);
						double sim = Double.parseDouble(split[1]);
						if (similarities.containsKey(movie1)){
							similarities.get(movie1).add(new Pair<Integer, Double>(movie2, sim));
						} else {
							similarities.put(movie1, new ArrayList<Pair<Integer, Double>>());
							similarities.get(movie1).add(new Pair<Integer, Double>(movie2, sim));
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
}