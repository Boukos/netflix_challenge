package algo1.job1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the SumReducer class from the word count exercise.
 */ 
public class CooccurrenceReducer extends Reducer<IDCompositeWritable, IDStatsWritable, IDPairWritable, Text> {
	
	private final Set<IDPairWritable> seenPairs = new HashSet<>();
	private final IDPairWritable keyOut = new IDPairWritable();
	private final Text valOut = new Text();

  @Override
  public void reduce(IDCompositeWritable key, Iterable<IDStatsWritable> values, Context context)
      throws IOException, InterruptedException {
	  System.out.println("in reducer");
	  System.out.println(key.getMovieid()+","+key.getUserid());
	  for (IDStatsWritable i: values) {
		  System.out.println(i.getId());
	  }
	  for (Iterator<IDStatsWritable> it1 = values.iterator(); it1.hasNext();) {
		  IDStatsWritable w1 = it1.next();
		  for (Iterator<IDStatsWritable> it2 = values.iterator(); it2.hasNext();) {
			  IDStatsWritable w2 = it2.next();
			  System.out.println(w1.getId());
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