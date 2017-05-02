package preprocessing.si;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {

	private static final Map<String, Integer> KEY_INDEX_TABLE = new HashMap<>();
	
	// the position of the key in the line
	private static int KEY_INDEX;
	
	// position of rating in the line
	private static int RATING_INDEX = 2;
	
	private final LongWritable keyOut = new LongWritable();
	private final DoubleWritable valOut = new DoubleWritable();
	
	static {
		// in a record, movieid is the first field and userid is the second
		KEY_INDEX_TABLE.put("movieid", 0);
		KEY_INDEX_TABLE.put("userid", 1);
	}
	
	@Override
	public void setup(Context context) {
		// set key index, default to movieid/0
		Configuration conf = context.getConfiguration();
		String key = conf.get("key", "movieid");
		Integer index = KEY_INDEX_TABLE.get(key);
		KEY_INDEX = index == null ? 0 : index;
	}
	
	/*
	 * emits either movieid or userid for the key, and the rating for the value
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		
		String[] vals = record.split(",");
		if (vals.length == 3) {
			long id = Long.parseLong(vals[KEY_INDEX]);
			double rating = Double.parseDouble(vals[RATING_INDEX]);
			
			keyOut.set(id);
			valOut.set(rating);
			
			context.write(keyOut, valOut);
		}
	}
}
