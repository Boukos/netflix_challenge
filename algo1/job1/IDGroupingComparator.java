package algo1.job1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IDGroupingComparator extends WritableComparator {

	// group on primary key only (userid)
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IDCompositeWritable key1 = (IDCompositeWritable)w1;
		IDCompositeWritable key2 = (IDCompositeWritable)w2;
		return key1.getUserid() - key2.getUserid();
	}
}
