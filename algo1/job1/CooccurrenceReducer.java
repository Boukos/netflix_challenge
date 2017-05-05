package algo1.job1;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the SumReducer class from the word count exercise.
 */ 
public class CooccurrenceReducer extends Reducer<IntWritable, IDStatsWritable, IDPairWritable, Text> {

	private final Set<IDPairWritable> seenPairs = new HashSet<>();
	private final IDPairWritable keyOut = new IDPairWritable();
	private final Text valOut = new Text();

	@Override
	public void reduce(IntWritable key, Iterable<IDStatsWritable> values, Context context)
			throws IOException, InterruptedException {
		List<IDStatsWritable> vals = new LinkedList<>();
		for (IDStatsWritable i: values) {
			vals.add(new IDStatsWritable(i.getId(), i.getSI(), i.getSumSI(), i.getNumSI()));
		}
		
		Collections.sort(vals, new Comparator<IDStatsWritable>() {
			public int compare(IDStatsWritable w1, IDStatsWritable w2) {
				return w1.getId() - w2.getId();
			}
		});
		
		System.out.println(vals.size());
		for (int i = 0; i < vals.size(); i++) {
			for (int j = i + 1; j < vals.size(); j++) {
				IDStatsWritable w1 = vals.get(i);
				IDStatsWritable w2 = vals.get(j);
				keyOut.setMovieID1(w1.getId());
				keyOut.setMovieID2(w2.getId());
				valOut.set(
						w1.getSI()
						+ "," + w1.getSumSI()
						+ "," + w1.getNumSI()
						+ "," + w2.getSI()
						+ "," + w2.getSumSI()
						+ "," + w2.getNumSI());
				context.write(keyOut, valOut);
			}
		}
	}
}