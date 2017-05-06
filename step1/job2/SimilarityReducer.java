package step1.job2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import step1.job1.IDPairWritable;
import step1.job1.StatsWritable;

public class SimilarityReducer extends Reducer<IDPairWritable, StatsPairWritable, IDPairWritable, DoubleWritable> {
	 @Override
	  public void reduce(IDPairWritable key, Iterable<StatsPairWritable> values, Context context)
	      throws IOException, InterruptedException {
		
		// variables for pearson correlation
		double sumNum = 0;
		double sumDenom1 = 0;
		double sumDenom2 = 0;
		
		// counter to find length of list
		int counter = 0;
//		System.out.println("KEY" + key);
//		System.out.println("COUNT: " + counter);
	    for (StatsPairWritable value : values) {
	    	counter++;
//			System.out.println("VAL" + value);
	    	StatsWritable stats1 = value.getStats1();
	    	StatsWritable stats2 = value.getStats2();
	    	
	    	// calculate r_x bar and r_y bar in algorithm
	    	double avg1 = stats1.getSumSI()/stats1.getNumSI();
	    	double avg2 = stats2.getSumSI()/stats2.getNumSI();
	    	
	    	// calculate the normalized ratings for each item
	    	double norm1diff = stats1.getSI()-avg1;
	    	double norm2diff = stats2.getSI()-avg2;
	    	
	    	// use normalized ratings to get components for pearson
	    	sumNum += norm1diff*norm2diff;
	    	sumDenom1 += norm1diff*norm1diff;
	    	sumDenom2 += norm2diff*norm2diff;

	    }
	    double corr = sumNum/(Math.sqrt(sumDenom1)*Math.sqrt(sumDenom2));
	    
	    if (counter > 1) {
//	    	System.out.println("Writing");
	    	context.write(key, new DoubleWritable(corr));
	    }
	  }
	 
}



