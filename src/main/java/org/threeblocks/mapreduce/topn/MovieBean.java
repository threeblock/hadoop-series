package org.threeblocks.mapreduce.topn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 分装电影的实体bean
 * mapreduce中需要序列化，该bean需要实现writable接口
 * 需要能进行比较，故也需要实现comparable接口
 * hadoop中封装了两者合并的writableComparable接口
 * 
 * @ClassName: MovieBean 
 * @Description: TODO 
 * @author chenlei 
 * @date 2018年8月8日 下午4:03:56 
 *
 */
public class MovieBean implements WritableComparable<MovieBean> {
	//{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
	private String movie;
	
	private int rate;
	
	private long timeStamp;
	
	private String uid;

	public String getMovie() {
		return movie;
	}

	public void setMovie(String movie) {
		this.movie = movie;
	}

	public int getRate() {
		return rate;
	}

	public void setRate(int rate) {
		this.rate = rate;
	}

	public long getTimeStamp() {
		return timeStamp;
	}
	
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}
	
	public MovieBean() {
	}

	public MovieBean(String movie, int rate, long timeStamp, String uid) {
		//super();
		this.movie = movie;
		this.rate = rate;
		this.timeStamp = timeStamp;
		this.uid = uid;
	}
	
	//反序列化
	@Override
	public void readFields(DataInput input) throws IOException {
		this.movie = input.readUTF();
		this.rate = input.readInt();
		this.timeStamp = input.readLong();
		this.uid = input.readUTF();
	}
	
	//序列化
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(this.movie);
		output.writeInt(this.rate);
		output.writeLong(this.timeStamp);
		output.writeUTF(this.uid);
	}

	@Override
	public int compareTo(MovieBean o) {
		//先比movieId,再比rate
		return this.movie.compareTo(o.getMovie()) == 0 ? (o.getRate() - this.rate) : this.movie.compareTo(o.getMovie());
	}

	@Override
	public String toString() {
		return "MovieBean [movie=" + movie + ", rate=" + rate + ", timeStamp=" + timeStamp + ", uid=" + uid + "]";
	}

	
}
