package org.threeblocks.mapreduce.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * 封装订单和用户的合并信息
 * @ClassName: JoinBean 
 * @Description: TODO 
 * @author chenlei 
 * @date 2018年8月8日 下午7:07:01 
 *
 */
public class JoinBean implements Writable {
	
	private String oid;
	
	private String uid;
	
	private String username;
	
	private String gender;
	
	public JoinBean() {
	}
	
	public JoinBean(String oid, String uid, String username, String gender) {
		super();
		this.oid = oid;
		this.uid = uid;
		this.username = username;
		this.gender = gender;
	}

	

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(oid);
		out.writeUTF(uid);
		out.writeUTF(username);
		out.writeUTF(gender);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.oid = in.readUTF();
		this.uid = in.readUTF();
		this.username = in.readUTF();
		this.gender = in.readUTF();
		
	}

	@Override
	public String toString() {
		return "JoinBean [oid=" + oid + ", uid=" + uid + ", username=" + username + ", gender=" + gender + "]";
	}
	
	
	
}
