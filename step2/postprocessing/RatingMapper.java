package step2.postprocessing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

	
private static final String[] FILES = {"avg_movie_ratings.txt", "avg_user_ratings.txt", "TestingRatings.txt"};
	
	private Map<Integer, Double> userAvgs = new HashMap<>();
	private Map<Integer, Double> movieAvgs = new HashMap<>();
	private Map<String, Double> testingRatings = new HashMap<>();
	
	private final Text keyOut = new Text();
	private final Text valOut = new Text();
	
	// computed in pig script
	private double overallAvg = 3.481186276307742;
	
	@Override
	public void setup(Context context) {
		// fill userAvgs and movieAvgs from files
		BufferedReader br = null;
		try {
			for (int i = 0; i < FILES.length; i++) {
				br = new BufferedReader(new FileReader(FILES[i]));
				String currLine;
				while ((currLine = br.readLine()) != null) {
					String[] vals = currLine.split("\t");
					if (vals.length == 2) {
						int id = Integer.parseInt(vals[0]);
						double rating = Double.parseDouble(vals[1]);
						if (i == 0) {
							movieAvgs.put(id, rating);
						}
						else if (i == 1) {
							userAvgs.put(id, rating);
						}
					}
					else {
						vals = currLine.split(",");
						if (vals.length == 3) {
							String ids = (vals[0] + "," + vals[1]).trim();
							Double rating = Double.parseDouble(vals[2]);
							testingRatings.put(ids, rating);
						}
					}
				}
				br.close();
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
		// userid, movieid, predicted_si
		String[] vals = value.toString().split(",");
		if (vals.length == 3) {
			double userPart = userAvgs.get(Integer.parseInt(vals[0])) - overallAvg;
			double moviePart = movieAvgs.get(Integer.parseInt(vals[1])) - overallAvg;
			double predictedSI = Double.parseDouble(vals[2]);
			double predictedRating = predictedSI + userPart + moviePart + overallAvg;
			String ids = (vals[1] + "," + vals[0]).trim();
			double actualRating = testingRatings.get(ids);
			
			keyOut.set(ids);
			valOut.set(predictedRating + "," + actualRating);
			
			context.write(keyOut, valOut);
		}
	}
}
