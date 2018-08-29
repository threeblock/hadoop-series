package org.threeblocks.mapreduce.join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 实现订单表和用户表信息的合并
 * @ClassName: Join 
 * @Description: TODO 
 * @author chenlei 
 * @date 2018年8月8日 下午7:04:27 
 *
 */
public class Join {
	/**
	 * 以uid为key，joinbean为value输出
	 * 难点：1、不知道maptask读取的哪种文件
	 * @ClassName: JoinMapper 
	 * @Description: TODO 
	 * @author chenlei 
	 * @date 2018年8月8日 下午7:11:29 
	 *
	 */
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			
			//1、获取maptask读取的文件切片信息，需声明是哪种类型的切片//
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			
			//2、获取文件名，根据文件名获取是哪种文件
			String filename = fileSplit.getPath().getName();
			
			JoinBean joinBean = new JoinBean();
			
			//空属性填充"NULL"
			if(filename.startsWith("order.txt")) {
				joinBean.setOid(split[1]);
				joinBean.setUid(split[0]);
				joinBean.setUsername("NULL");
				joinBean.setGender("NULL");
			} else {
				joinBean.setOid("NULL");
				joinBean.setUid(split[0]);
				joinBean.setUsername(split[1]);
				joinBean.setGender(split[2]);
			}
			
			context.write(new Text(joinBean.getUid()), joinBean);
		}
		
	}
	
	public static class JoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<JoinBean> values,
				Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context) throws IOException, InterruptedException {
			//用于存放订单信息的joinbean
			ArrayList<JoinBean> arrayList = new ArrayList<JoinBean>();
			JoinBean joinBean = new JoinBean();
			//循环将order的joinBean放入list
			for (JoinBean jb : values) {
				if("NULL".equals(jb.getOid())) {
					joinBean.setUid(jb.getUid());
					joinBean.setOid(jb.getOid());
					joinBean.setUsername(jb.getUsername());
					joinBean.setGender(jb.getGender());
				} else {
					JoinBean newBean = new JoinBean();
					newBean.setUid(jb.getUid());
					newBean.setOid(jb.getOid());
					newBean.setUsername(jb.getUsername());
					newBean.setGender(jb.getGender());
					arrayList.add(newBean);
				}
				
			}
			
			for (JoinBean jb1 : arrayList) {
				JoinBean j = new JoinBean();
				j.setOid(jb1.getOid());
				j.setUid(joinBean.getUid());
				j.setUsername(joinBean.getUsername());
				j.setGender(joinBean.getGender());
				context.write(j, NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		
		org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(configuration);
		
		job.setJarByClass(Join.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinBean.class);
		
		Path path = new Path("D:\\mrtest\\join\\output");
		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(path)) {
			fs.delete(path, true);
		}
		
		FileInputFormat.setInputPaths(job, new Path("D:\\mrtest\\join\\input"));
		FileOutputFormat.setOutputPath(job, path);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
