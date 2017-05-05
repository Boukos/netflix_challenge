package algo1.job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StatsWritable implements Writable {
	
	double SI;
	double sumSI;
	int numSI;
	
	public StatsWritable() {
		
	}
	
	public StatsWritable(double SI, double sumSI, int numSI) {
		this.SI = SI;
		this.sumSI = sumSI;
		this.numSI = numSI;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		SI = in.readDouble();
		sumSI = in.readDouble();
		numSI = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(SI);
		out.writeDouble(sumSI);
		out.writeInt(numSI);
	}

	public double getSI() {
		return SI;
	}

	public void setSI(double sI) {
		SI = sI;
	}

	public double getSumSI() {
		return sumSI;
	}

	public void setSumSI(double sumSI) {
		this.sumSI = sumSI;
	}

	public int getNumSI() {
		return numSI;
	}

	public void setNumSI(int numSI) {
		this.numSI = numSI;
	}
	
	public String toString() {
		return getSI() + "," + getSumSI() + "," + getNumSI();
	}

}
