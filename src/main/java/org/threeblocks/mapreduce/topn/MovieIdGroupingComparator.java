package org.threeblocks.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * reduce task使用的分组比较器，使不同maptask程序处理的同一分区的数据可以合为一组
 * reduce task的迭代器就是用这个比较器来比较两个相邻的key，是否应该看成同一组
 * 
 * @ClassName: MovieIdGroupingComparator 
 * @Description: TODO 
 * @author chenlei 
 * @date 2018年8月8日 下午5:09:43 
 *
 */
public class MovieIdGroupingComparator extends  WritableComparator {
	
	public MovieIdGroupingComparator() {
		super(MovieBean.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//此处采用了反射，若是要实现这个功能，必须先声明上述的构造函数
		MovieBean key1 = (MovieBean) a;
		MovieBean key2 = (MovieBean) b;
		return key1.getMovie().compareTo(key2.getMovie());
	}
}
