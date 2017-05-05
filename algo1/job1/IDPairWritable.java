package algo1.job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IDPairWritable implements WritableComparable<IDPairWritable> {
	
	int movieID1;
	int movieID2;
	
	public IDPairWritable() {
		
	}
	
	public IDPairWritable(int movieID1, int movieID2) {
		this.movieID1 = movieID2;
		this.movieID2 = movieID2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		movieID1 = in.readInt();
		movieID2 = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(movieID1);
		out.writeInt(movieID2);
	}

	@Override
	public int compareTo(IDPairWritable o) {
		int dif1 = movieID1 - o.getMovieID1();
		if (dif1 == 0) {
			return movieID2 - o.getMovieID2();
		}
		return dif1;
	}
	
	@Override
	public int hashCode() {
		return movieID1 ^ movieID2;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IDPairWritable)) {
			return false;
		}
		IDPairWritable other = (IDPairWritable)o;
		
		// ignore order
		return (movieID1 == other.getMovieID1()) && (movieID2 == other.getMovieID2())
				|| (movieID1 == other.getMovieID2()) && (movieID2 == other.getMovieID1());
	}

	public int getMovieID1() {
		return movieID1;
	}

	public void setMovieID1(int movieID1) {
		this.movieID1 = movieID1;
	}

	public int getMovieID2() {
		return movieID2;
	}

	public void setMovieID2(int movieID2) {
		this.movieID2 = movieID2;
	}

}
