package algo1.job2;


import algo1.job1.StatsWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StatsPairWritable implements WritableComparable<StatsPairWritable> {
	
	StatsWritable stats1 = new StatsWritable();
	StatsWritable stats2 = new StatsWritable();
	
	public StatsPairWritable() {
		
	}
	
	public StatsPairWritable(StatsWritable stats1, StatsWritable stats2) {
		this.stats1 = stats2;
		this.stats2 = stats2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		stats1.setSI(in.readDouble());
		stats1.setSumSI(in.readDouble());
		stats1.setNumSI(in.readInt());
		stats2.setSI(in.readDouble());
		stats2.setSumSI(in.readDouble());
		stats2.setNumSI(in.readInt());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(stats1.getSI());
		out.writeDouble(stats1.getSumSI());
		out.writeInt(stats1.getNumSI());
		out.writeDouble(stats2.getSI());
		out.writeDouble(stats2.getSumSI());
		out.writeInt(stats2.getNumSI());
	}

	@Override
	public int compareTo(StatsPairWritable o) {
		double dif1 = stats1.getSI() - o.getStats1().getSI();
		if (dif1 == 0) {
			return (int) (stats2.getSI() - o.getStats2().getSI());
		}
		return (int)dif1;
	}

	public StatsWritable getStats1() {
		return stats1;
	}

	public void setStats1(StatsWritable stats1) {
		this.stats1 = stats1;
	}

	public StatsWritable getStats2() {
		return stats2;
	}

	public void setStats2(StatsWritable stats2) {
		this.stats2 = stats2;
	}
	
	public String toString() {
		return stats1 + "," + stats2;
	}

}
