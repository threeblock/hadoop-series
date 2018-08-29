package org.threeblocks.mapreduce.topn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 找出每部电影评分前2的数据
 * @ClassName: Movie_topn 
 * @Description: TODO 
 * @author chenlei 
 * @date 2018年8月8日 下午4:02:22 
 *
 */
public class Movie_topn {
	
	public static class MovieMapper extends Mapper<LongWritable, Text, MovieBean, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, MovieBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//使用try，catch处理脏数据
			try {
				//1、切分每一行，json类型{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
				String json = value.toString();
				if("".equals(json)) {
					return;
				}
				//json解析工具
				ObjectMapper objectMapper = new ObjectMapper();
				MovieBean movieBean = objectMapper.readValue(json, MovieBean.class);
				
				//写入context,maptask调用的时候已经可以使用moviebean的排序了，还需要实现maptask自定义的分区
				context.write(movieBean, NullWritable.get());
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
	
	/*
	 * 自定义电影评分排序topn的reduce类
	 */
	public static class MovieReducer extends Reducer<MovieBean, NullWritable, MovieBean, NullWritable> {
		@Override
		protected void reduce(MovieBean key, Iterable<NullWritable> values,
				Reducer<MovieBean, NullWritable, MovieBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//只需输出前几条即可
			int topn = 0;
			for (NullWritable v : values) {
				context.write(key, v);
				topn ++;
				if(topn == 2) {
					return;
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		//System.setProperty("HADOOP_USER_NAME", "root");//避免将windows用户名提交到Linux中
		
		//1、配置
		Configuration conf = new Configuration();
		//不写的话默认本地
		//conf.set("mapreduce.framework.name", "yarn");
		//conf.set("fs.defaultFS", "hdfs://cts01:9000");
		
		
		//2、生成jobclient实例
		org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf);
		job.setJarByClass(Movie_topn.class);
		
		job.setMapperClass(MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		
		//告诉采用自定义的分区分组
		//分区组件由maptask用
		job.setPartitionerClass(MovieIdPartitioner.class);
		//分组组件由reducetask用
		job.setGroupingComparatorClass(MovieIdGroupingComparator.class);
		
		job.setMapOutputKeyClass(MovieBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//job.setOutputKeyClass(MovieBean.class);
		//job.setOutputValueClass(NullWritable.class);
		
		//输出路径存在的话会报错，判断输出路径是否存在，若存在删除
		Path path = new Path("D:\\mrtest\\movie_topn\\output");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(path)) {
			fs.delete(path, true);
		}
		
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, "D:\\mrtest\\movie_topn\\input");
		FileOutputFormat.setOutputPath(job, path);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
