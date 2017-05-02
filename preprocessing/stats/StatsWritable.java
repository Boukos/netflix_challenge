package preprocessing.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StatsWritable implements WritableComparable<StatsWritable> {
	
	int numSI;
	double sumSI;
	
	public StatsWritable() { }
	
	public StatsWritable(int numSI, double sumSI) {
		this.numSI = numSI;
		this.sumSI = sumSI;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		numSI = in.readInt();
		sumSI = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(numSI);
		out.writeDouble(sumSI);
	}

	@Override
	public int compareTo(StatsWritable o) {
		int difCount = this.numSI - o.numSI;
		if (difCount == 0) {
			return (int)((this.sumSI - o.sumSI) * 100);
		}
		return difCount;
	}
	
	@Override
	public String toString() {
		return numSI + "," + sumSI;
	}
}
