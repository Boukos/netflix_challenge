package step2.topN;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IDSIWritable implements WritableComparable<IDSIWritable> {
	
	int movieID;
	double SI;
	
	public IDSIWritable() {
		
	}
	
	public IDSIWritable(int movieID, double SI) {
		this.movieID = movieID;
		this.SI = SI;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		movieID = in.readInt();
		SI = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(movieID);
		out.writeDouble(SI);
	}

	@Override
	public int compareTo(IDSIWritable o) {
		int dif1 = movieID - o.getmovieID();
		if (dif1 == 0) {
			return (int)(SI - o.getSI());
		}
		return dif1;
	}
	
	@Override
	public int hashCode() {
		return movieID ^ (int)SI;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IDSIWritable)) {
			return false;
		}
		IDSIWritable other = (IDSIWritable)o;
		
		// ignore order
		return (movieID == other.getmovieID()) && (SI == other.getSI())
				|| (movieID == other.getSI()) && (SI == other.getmovieID());
	}

	public int getmovieID() {
		return movieID;
	}

	public void setmovieID(int movieID) {
		this.movieID = movieID;
	}

	public double getSI() {
		return SI;
	}

	public void setSI(double SI) {
		this.SI = SI;
	}

}
