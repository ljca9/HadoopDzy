package com.mazh.aura.clickstream;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import com.mazh.aura.mrbean.WebLogBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 将清洗之后的日志梳理出点击流pageviews模型数据
 *
 * 输入数据是清洗过后的结果数据
 *
 * 区分出每一次会话，给每一次visit（session）增加了session-id（随机uuid）
 * 梳理出每一次会话中所访问的每个页面（请求时间，url，停留时长，以及该页面在这次session中的序号）
 * 保留referral_url，body_bytes_send，useragent
 *
 * 结果数据：
 * 745a8b10-d4d9-4d8b-9b09-2b3b299877b8174.120.8.226-2013-09-18 13:22:30/hadoop-mahout-roadmap/16"-""WordPress/3.3.1;http://www.getonboardbc.com"0200
 * 745a8b10-d4d9-4d8b-9b09-2b3b299877b8174.120.8.226-2013-09-18 13:22:36/hadoop-mahout-roadmap/2196"-""WordPress/3.3.1;http://www.getonboardbc.com"0200
 * 745a8b10-d4d9-4d8b-9b09-2b3b299877b8174.120.8.226-2013-09-18 13:25:52/hadoop-mahout-roadmap/371"-""WordPress/3.3.1;http://www.getonboardbc.com"0200
 * 745a8b10-d4d9-4d8b-9b09-2b3b299877b8174.120.8.226-2013-09-18 13:27:03/hadoop-mahout-roadmap/460"-""WordPress/3.3.1;http://www.getonboardbc.com"0200
 */
public class ClickStreamPageView {

    static class ClickStreamMapper extends Mapper<LongWritable, Text, Text, WebLogBean> {

        Text k = new Text();
        WebLogBean v = new WebLogBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = line.split("\001");
            if (fields.length < 9) return;

            //将切分出来的各字段set到weblogbean中
            v.set("true".equals(fields[0]) ? true : false, fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);

            //只有有效记录才进入后续处理 key ip value:剩余的状态为true的：一个对象
            //ip相同的为一组
            if (v.isValid()) {
                k.set(v.getRemote_addr());
                context.write(k, v);
            }
        }
    }

    static class ClickStreamReducer extends Reducer<Text, WebLogBean, NullWritable, Text> {

        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<WebLogBean> beans = new ArrayList<>();

            // 将所有 WebLogBean 加入到列表中
            try {
                for (WebLogBean bean : values) {
                    WebLogBean webLogBean = new WebLogBean();
                    try {
                        BeanUtils.copyProperties(webLogBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    beans.add(webLogBean);
                }

                // 按照时间排序
                Collections.sort(beans, new Comparator<WebLogBean>() {
                    @Override
                    public int compare(WebLogBean o1, WebLogBean o2) {
                        try {
                            Date d1 = toDate(o1.getTime_local());
                            Date d2 = toDate(o2.getTime_local());
                            if (d1 == null || d2 == null) return 0;
                            return d1.compareTo(d2);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return 0;
                        }
                    }
                });

                int step = 1;
                String session = UUID.randomUUID().toString();  // 生成 session ID

                // 处理每一条记录
                for (int i = 0; i < beans.size(); i++) {
                    WebLogBean bean = beans.get(i);

                    if (i == 0) continue;  // 跳过第一条记录

                    long timeDiff = timeDiff(toDate(bean.getTime_local()), toDate(beans.get(i - 1).getTime_local()));

                    // 判断时间差来分配 session
                    if (timeDiff < 30 * 60 * 1000) {
                        // 如果时间差小于30分钟，属于同一个 session
                        v.set(session + "\001" + key.toString() + "\001" + beans.get(i - 1).getRemote_user() + "\001"
                                + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001"
                                + step + "\001" + (timeDiff / 1000) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
                                + beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent()
                                + "\001" + beans.get(i - 1).getStatus());
                        context.write(NullWritable.get(), v);
                        step++;
                    } else {
                        // 如果时间差大于30分钟，属于不同 session，生成新的 session
                        v.set(session + "\001" + key.toString() + "\001" + beans.get(i - 1).getRemote_user() + "\001"
                                + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001"
                                + step + "\001" + 60 + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
                                + beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent()
                                + "\001" + beans.get(i - 1).getStatus());
                        context.write(NullWritable.get(), v);
                        step = 1;  // 重置 step 计数
                        session = UUID.randomUUID().toString();  // 新的 session ID
                    }

                    // 如果是最后一条数据
                    if (i == beans.size() - 1) {
                        v.set(session + "\001" + key.toString() + "\001" + bean.getRemote_user() + "\001"
                                + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step
                                + "\001" + 60 + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent()
                                + "\001" + bean.getBody_bytes_sent() + "\001" + bean.getStatus());
                        context.write(NullWritable.get(), v);
                    }
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        private Date toDate(String timeStr) throws ParseException {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
            return df.parse(timeStr);
        }

        private long timeDiff(Date time1, Date time2) {
            return time1.getTime() - time2.getTime();
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建配置对象
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        // 检查命令行参数是否正确
        if (otherArgs.length < 2) {
            System.err.println("Usage: count <in> [<in>...] <out>");
            System.exit(2);
        }

        // 设置 HDFS 和 YARN 配置
        configuration.set("fs.defaultFS", "hdfs://master:9000");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        configuration.set("yarn.resourcemanager.hostname", "master");
        System.setProperty("HADOOP_USER_NAME", "root");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ClickStreamPageView.class);

        job.setMapperClass(ClickStreamMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);

        job.setReducerClass(ClickStreamReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);  // 设置 reducer 数量为 0，所有计算在 mapper 端完成

        // 添加输入路径
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // 输出路径设置
        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);  // 删除已存在的输出路径
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone ? 0 : 1);
    }
}
