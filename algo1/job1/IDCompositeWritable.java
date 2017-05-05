package algo1.job1;

import preprocessing.si.IDPairWritable;

/*
 * Sorry, this is pretty spaghetti. I needed the (movieid, userid) pair to use as a composite key, and the name IDPairWritable already exists in this package
 */

public class IDCompositeWritable extends IDPairWritable {
	
	public IDCompositeWritable() { }
	
	public IDCompositeWritable(int movieid, int userid) {
		super(movieid, userid);
	}
	
}
