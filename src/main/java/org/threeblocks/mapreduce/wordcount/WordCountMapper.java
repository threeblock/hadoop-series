package org.threeblocks.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 传入的key、value类型需要序列化，而实现serializable接口会比较臃肿
 * 故在hadoop中有专门的序列化包装类
 * String-->Text
 * int-->IntWritable
 * long-->LongWritable
 * 
 * @ClassName: MyMapper 
 * @Description: 实现wordcount的map程序 
 * @author chenlei 
 * @date 2018年8月7日 下午9:07:02 
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//每次读取一行时,key为每行的偏移量，value为每一行
		//1、切割每一行
		String[] words = value.toString().split(" ");
		
		//2、本方法没有返回值，故需要将返回的map封装在context中
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
		
	}
}
