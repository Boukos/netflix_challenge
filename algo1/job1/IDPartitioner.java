package algo1.job1;

import org.apache.hadoop.mapreduce.Partitioner;

public class IDPartitioner extends
		Partitioner<IDCompositeWritable, IDStatsWritable> {

	@Override
	public int getPartition(IDCompositeWritable key, IDStatsWritable value,
			int numPartitions) {
		// partition on natural key, userid
		return Math.abs(key.getUserid() % numPartitions);
	}

}
