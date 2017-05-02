package preprocessing.si;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class SIMapper extends Mapper<LongWritable, Text, IDPairWritable, DoubleWritable> {

	private static final String[] FILES = {"avg_movie_ratings.txt", "avg_user_ratings.txt"};
	
	private Map<Integer, Double> userAvgs = new HashMap<>();
	private Map<Integer, Double> movieAvgs = new HashMap<>();
	
	// computed in pig script
	private double overallAvg = 3.481187595074204;
	
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
						else {
							userAvgs.put(id, rating);
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
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		String[] vals = record.split(",");
		if (vals.length == 3) {
			int movieid = Integer.parseInt(vals[0]);
			int userid = Integer.parseInt(vals[1]);
			double rating = Double.parseDouble(vals[2]);
			
			double userPart = userAvgs.get(userid) - overallAvg;
			double moviePart = movieAvgs.get(movieid) - overallAvg;
			
			double specificInteraction = rating - overallAvg - userPart - moviePart;
			
			context.write(new IDPairWritable(movieid, userid), new DoubleWritable(specificInteraction));
		}
	}
}
