package algo1.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class CooccurrenceMapper extends Mapper<LongWritable, Text, IDCompositeWritable, IDStatsWritable> {
	

	private static final String SIDE_DATA = "stats.txt";

	// maps movieid to (numSI, sumSI)
	private final Map<Integer, Pair<Integer, Double>> stats = new HashMap<>();
	
	@Override
	public void setup(Context context) {
		// load stats into memory
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(SIDE_DATA));
			String currLine;
			while ((currLine = br.readLine()) != null) {
				String[] split = currLine.split("\t");
				if (split.length == 2) {
					String[] vals = split[1].split(",");
					if (vals.length == 2) {
						int movieid = Integer.parseInt(split[0]);
						int numSI = Integer.parseInt(vals[0]);
						double sumSI = Double.parseDouble(vals[1]);
						stats.put(movieid, new Pair<>(numSI, sumSI));
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
	
	/*
	 * receives a line from the SI matrix and emits (user_id, (item_id, stats))
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		if (line.length == 2) {
			String[] ids = line[0].split(",");
			int movieID = Integer.parseInt(ids[0]);
			int userID = Integer.parseInt(ids[1]);
			double SI = Double.parseDouble(line[1]);
			
			int numSI = stats.get(movieID).getFirst();
			double sumSI = stats.get(movieID).getSecond();
			
			context.write(new IDCompositeWritable(movieID, userID), new IDStatsWritable(userID, SI, sumSI, numSI));
		}
	}
}
