package algo1.job1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IDSortComparator extends WritableComparator {

	
	// this is almost certainly the behavior of the default sort comparator, but just in case
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		return w1.compareTo(w2);
	}
}
