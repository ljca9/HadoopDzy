package com.mazh.aura.mrbean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WLB implements Writable {
    private boolean valid=true;//判断数据是否合法
    private  String remote_addr;//记录客户端的ip地址
    private String remote_user;//记录客户端的用户名称，忽略属性","
    private String time_local;//记录访问时间与地区
    private String requests;//请求的资源
    private String status;//HTTP状态码
    private String body_bytes_sent;//发生的字节数
    private String http_referer;//Referer
    private String http_user_agent;//用户代理

    @Override
    public String toString() {
//  toString 方法将 WeblogBean 对象的各个字段拼接成一个字符串，并使用 \001 作为分隔符，这样可以方便在 Hadoop 中进行数据传输或存储。
        return this.valid +
                "\001" + this.getRemote_addr() +
                "\001" + this.getRemote_user() +
                "\001" + this.getTime_local() +
                "\001" + this.getRequests() +
                "\001" + this.getStatus() +
                "\001" + this.getBody_bytes_sent() +
                "\001" + this.getHttp_referer() +
                "\001" + this.getHttp_user_agent() +
                '}';
    }

    @Override
    //write 方法将 WeblogBean 对象的各个字段写入输出流（如 HDFS），在传输数据时保证序列化。
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(this.valid);//写入 valid 字段
        out.writeUTF(null==remote_addr?"":remote_addr);// 如果字段为 null，则写空字符串
        out.writeUTF(null==remote_user?"":remote_user);
        out.writeUTF(null==time_local?"":time_local);
        out.writeUTF(null==requests?"":requests);
        out.writeUTF(null==status?"":status);
        out.writeUTF(null==body_bytes_sent?"":body_bytes_sent);
        out.writeUTF(null==http_referer?"":http_referer);
        out.writeUTF(null==http_user_agent?"":http_user_agent);

    }

    public void set(boolean valid,String remote_addr, String remote_user, String time_local, String request, String status, String body_bytes_sent, String http_referer, String http_user_agent) {
        this.valid = valid;
        this.remote_addr = remote_addr;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.requests = requests;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_referer = http_referer;
        this.http_user_agent = http_user_agent;
    }
    @Override
//  readFields 方法从输入流中读取数据，并还原为 WeblogBean 对象，反序列化过程。
    public void readFields(DataInput in) throws IOException {
        this.valid=in.readBoolean();// 读取 valid 字段
        this.remote_addr=in.readUTF();// 读取各个字段
        this.remote_user=in.readUTF();
        this.time_local=in.readUTF();
        this.requests=in.readUTF();
        this.status=in.readUTF();
        this.body_bytes_sent=in.readUTF();
        this.http_referer=in.readUTF();
        this.http_user_agent=in.readUTF();
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }


    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequests() {
        return requests;
    }

    public void setRequests(String requests) {
        this.requests = requests;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

}
