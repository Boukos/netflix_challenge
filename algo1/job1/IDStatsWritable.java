package algo1.job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IDStatsWritable extends StatsWritable {
	
	private int id;
	
	public IDStatsWritable() { }
	
	public IDStatsWritable(int id, double SI, double sumSI, int numSI) {
		super(SI, sumSI, numSI);
		this.id = id;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		id = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(id);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public String toString() {
		return id + "," + getSI() + "," + getSumSI() + "," + getNumSI();
	}
}
