package algo1.job1;

/*
 * Sorry, this is pretty spaghetti. I needed the (movieid, userid) pair to use as a composite key, and the name IDPairWritable already exists in this package
 */

public class IDCompositeWritable extends preprocessing.si.IDPairWritable {
	
	public IDCompositeWritable(int movieid, int userid) {
		super(movieid, userid);
	}
	
}
