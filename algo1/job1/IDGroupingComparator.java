package algo1.job1;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IDGroupingComparator extends WritableComparator {
	
	public IDGroupingComparator() {
		super(IDCompositeWritable.class);
	}

	// group on primary key only (userid)
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IDCompositeWritable key1 = (IDCompositeWritable)w1;
		IDCompositeWritable key2 = (IDCompositeWritable)w2;
		return key1.getUserid() - key2.getUserid();
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DataInput stream1 = new DataInputStream(new ByteArrayInputStream(b1,
				s1, l1));
		DataInput stream2 = new DataInputStream(new ByteArrayInputStream(b2,
				s2, l2));

		IDCompositeWritable v1 = new IDCompositeWritable();
		IDCompositeWritable v2 = new IDCompositeWritable();

		try {
			v1.readFields(stream1);
			v2.readFields(stream2);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return compare(v1, v2);
	}
}
