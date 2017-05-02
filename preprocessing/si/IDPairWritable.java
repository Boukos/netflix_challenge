package preprocessing.si;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IDPairWritable implements WritableComparable<IDPairWritable> {
	
	private int movieid;
	private int userid;
	
	public IDPairWritable() { }
	
	public IDPairWritable(int movieid, int userid) {
		this.movieid = movieid;
		this.userid = userid;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		movieid = in.readInt();
		userid = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(movieid);
		out.writeInt(userid);
	}

	@Override
	public int compareTo(IDPairWritable other) {
		int movieDif = movieid - other.getMovieid();
		if (movieDif == 0) {
			return userid - other.getUserid();
		}
		return movieDif;
	}

	public int getMovieid() {
		return movieid;
	}

	public void setMovieid(int movieid) {
		this.movieid = movieid;
	}

	public int getUserid() {
		return userid;
	}

	public void setUserid(int userid) {
		this.userid = userid;
	}
	
	public String toString() {
		return movieid + "," + userid;
	}

}
