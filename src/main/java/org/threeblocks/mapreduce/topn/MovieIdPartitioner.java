package org.threeblocks.mapreduce.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区实现，使movieBean中movie相同的为一区
 * @ClassName: MovieIdPartitioner 
 * @Description: TODO 
 * @author chenlei 
 * @date 2018年8月8日 下午5:06:18 
 *
 */
public class MovieIdPartitioner extends Partitioner<MovieBean, NullWritable>{
	@Override
	public int getPartition(MovieBean key, NullWritable value, int numPartitions) {
		return (key.getMovie().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
