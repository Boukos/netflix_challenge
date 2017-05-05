package algo1.job1;

import java.io.IOException;
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

	private final List<IDStatsWritable> vals = new LinkedList<>();
	private final Set<IDPairWritable> seenPairs = new HashSet<>();
	private final IDPairWritable keyOut = new IDPairWritable();
	private final Text valOut = new Text();

	@Override
	public void reduce(IntWritable key, Iterable<IDStatsWritable> values, Context context)
			throws IOException, InterruptedException {
		for (IDStatsWritable i: values) {
			vals.add(new IDStatsWritable(i.getId(), i.getSI(), i.getSumSI(), i.getNumSI()));
		}
		for (Iterator<IDStatsWritable> it1 = vals.iterator(); it1.hasNext();) {
			IDStatsWritable w1 = it1.next();
			for (Iterator<IDStatsWritable> it2 = vals.iterator(); it2.hasNext();) {
				IDStatsWritable w2 = it2.next();
				if (w1.getId() != w2.getId()) {
					keyOut.setMovieID1(w1.getId());
					keyOut.setMovieID2(w2.getId());
					if (!seenPairs.contains(keyOut)) {
						seenPairs.add(new IDPairWritable(keyOut.getMovieID1(), keyOut.getMovieID2()));
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
	}
}