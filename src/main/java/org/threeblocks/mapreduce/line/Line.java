package org.threeblocks.mapreduce.line;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 数据格式为：
 * 1,4
 * 2,5
 * 3,4
 * 需求：如何求出整点出现的次数最多的数
 * 分析：类似于与Wordcount，如将1,4分成1,2,3,4，生成以这些数作为key，1作为value的map
 * @ClassName: Line 
 * @Description: TODO 
 * @author chenlei 
 * @date 2018年8月8日 下午2:04:34 
 *
 */
public class Line {
	
	//实现map的内部类
	public static class LineMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			//1、切分每一行
			String[] split = value.toString().split(",");
			
			//2、遍历得到每一行中的所有整点数，形成map放到context中
			for(int i=Integer.parseInt(split[0]);i<=Integer.parseInt(split[1]);i++) {
				context.write(new IntWritable(i), new IntWritable(1));
			}
			
		}
	}
	
	//实现reduce的内部类
	public static class LineReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//为每一个分组定义一个初始计数器
			int count = 0;
			
			//循环处理每次加1
			for (IntWritable v : values) {
				count += v.get();
			}
			
			//找出有重合的整点数，写到context中去
			if(count > 1) {
				context.write(key, new IntWritable(count));
			}
		}
	}
	
	//jobClient程序
	public static void main(String[] args) throws Exception {
		
		//System.setProperty("HADOOP_USER_NAME", "root");//避免将windows用户名提交到Linux中
		
		//1、配置
		Configuration conf = new Configuration();
		//不写的话默认本地
		//conf.set("mapreduce.framework.name", "yarn");
		//conf.set("fs.defaultFS", "hdfs://cts01:9000");
		
		
		//2、生成jobclient实例
		org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf);
		job.setJarByClass(Line.class);
		
		job.setMapperClass(LineMapper.class);
		job.setReducerClass(LineReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//FileInputFormat.setInputPaths(job, "D:\\mrtest\\input");
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, "D:\\mrtest\\input");
		//FileOutputFormat.setOutputPath(job, "D:\\mrtest\\output");
		FileOutputFormat.setOutputPath(job, new Path("D:\\mrtest\\output"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
