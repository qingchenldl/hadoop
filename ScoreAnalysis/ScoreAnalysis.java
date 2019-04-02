/*
* @_Author_: V0W
* @_Date_: 4.1
* @运行时参数：<输入文件夹><输出文件夹>
* @作者运行时参数：hdfs://192.168.118.144:9000/user/ldl/input/class3
                    hdfs://192.168.118.144:9000/user/ldl/output
* @_res_: 分别统计各个班级的最值成绩，平均成绩，ABCDE等级人数
*          和全部班级的最值成绩，平均成绩，ABCDE等级人数
* */
package liudl;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class ScoreAnalysis {
    private static String SPACE = "\t";
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                String name = tokenizer.nextToken();    //姓名
                String classNo = tokenizer.nextToken(); //班级
                String score = tokenizer.nextToken();   //成绩
                String stuScore = name+SPACE+score;    //姓名成绩作为value,班级作为key
                context.write(new Text(classNo), new Text(stuScore));
            }
        }
    }

    public static class Reduce extends
            Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int classNo = Integer.parseInt(key.toString());
            int sum = 0, avg, stuNum = 0;
            int max = -1, min = 150;
            int A=0, B=0, C=0, D=0, E=0;
            List<String>  cache = new ArrayList<>();    //存放keyvalue，用于下一步找对应学生

            for(Text val : values){
                cache.add(val.toString());
                String[] valTokens = val.toString().split(SPACE);
                int score = Integer.parseInt(valTokens[1]);
                if(max < score){
                    max = score;
                }
                if(min > score){
                    min = score;
                }
                sum += score;
                stuNum ++;
            }
            avg = sum / stuNum;
            context.write(new Text("classNo:"), new IntWritable(classNo));
            context.write(new Text("The average is:"), new IntWritable(avg));
            context.write(new Text("min:"), new IntWritable(min));
            for (String val : cache){
                int score = Integer.parseInt(val.split(SPACE)[1]);
                if(score == min){
                    String name = val.split(SPACE)[0];
                    String text = name + SPACE + key.toString();
                    context.write(new Text(text), new IntWritable(min));
                }
            }
            context.write(new Text("max:"), new IntWritable(max));
            for (String val : cache){
                int score = Integer.parseInt(val.split(SPACE)[1]);
                if(score == max){
                    String name = val.split(SPACE)[0];
                    String text = name + SPACE + key.toString();
                    context.write(new Text(text), new IntWritable(max));
                }
            }
            for (String val : cache){
                int score = Integer.parseInt(val.split(SPACE)[1]);
                if ((score>= 90)&&(score <=100)) {
                    A ++;
                }else if((score >=80)&&(score< 90)) {
                    B ++;
                }else if((score >=70)&&(score< 80)){
                    C ++;
                }else if((score >= 60)&&(score<70)){
                    D ++;
                }else{
                    E ++;
                }
            }
            context.write(new Text("A Num:"), new IntWritable(A));
            context.write(new Text("B Num:"), new IntWritable(B));
            context.write(new Text("C Num:"), new IntWritable(C));
            context.write(new Text("D Num:"), new IntWritable(D));
            context.write(new Text("E Num:"), new IntWritable(E));
            context.write(new Text("-----------------------------------------------"), new IntWritable(classNo));
        }
    }

    public static class AllMap extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                String name = tokenizer.nextToken();    //姓名
                String classNo = tokenizer.nextToken(); //班级
                int score = Integer.parseInt(tokenizer.nextToken());   //成绩
                String stuInfo = classNo+SPACE+name+SPACE+score;    //姓名班级作为value,成绩作为key
                context.write(new IntWritable(0), new Text(stuInfo));
            }
        }
    }

    public static class AllReduce extends
            Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0, avg, stuNum = 0;
            int max = -1, min = 150;
            int A=0, B=0, C=0, D=0, E=0;
            List<String>  cache = new ArrayList<>();    //存放keyvalue，用于下一步找对应学生

            for(Text val : values){
                cache.add(val.toString());
                String[] valTokens = val.toString().split(SPACE);
                int score = Integer.parseInt(valTokens[2]);
                if(max < score){
                    max = score;
                }
                if(min > score){
                    min = score;
                }
                sum += score;
                stuNum ++;
            }
            avg = sum / stuNum;
            context.write(new Text("The average is:"), new IntWritable(avg));
            context.write(new Text("min:"), new IntWritable(min));
            for (Text val : values){
                String[] valTokens = val.toString().split(SPACE);
                int score = Integer.parseInt(valTokens[2]);
                String stuInfo = valTokens[0]+SPACE+valTokens[1];
                if(score == min){
                    context.write(new Text(stuInfo), new IntWritable(min));
                }
            }
            context.write(new Text("max:"), new IntWritable(max));
            for (Text val : values){
                String[] valTokens = val.toString().split(SPACE);
                int score = Integer.parseInt(valTokens[2]);
                String stuInfo = valTokens[0]+SPACE+valTokens[1];
                if(score == max){
                    context.write(new Text(stuInfo), new IntWritable(max));
                }
            }

            for (String val : cache){
                String[] valTokens = val.split(SPACE);
                int score = Integer.parseInt(valTokens[2]);
                if ((score>= 90)&&(score <=100)) {
                    A ++;
                }else if((score >=80)&&(score< 90)) {
                    B ++;
                }else if((score >=70)&&(score< 80)){
                    C ++;
                }else if((score >= 60)&&(score<70)){
                    D ++;
                }else{
                    E ++;
                }
            }
            context.write(new Text("A Num:"), new IntWritable(A));
            context.write(new Text("B Num:"), new IntWritable(B));
            context.write(new Text("C Num:"), new IntWritable(C));
            context.write(new Text("D Num:"), new IntWritable(D));
            context.write(new Text("E Num:"), new IntWritable(E));
            context.write(new Text("-----------------------------------------------"), new IntWritable(0));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        String avgClassPath = otherArgs[1]+"/class";
        String avgAllPath = otherArgs[1]+"/all";

        if (otherArgs.length != 2) { // 判断路径参数是否为2个
            System.err.println("Usage: Data Deduplication <in> <out>");
            System.exit(2);
        }
        // 统计每个班级的成绩信息
        Job job = new Job(conf, "ScoreAnalysis");
        job.setJarByClass(ScoreAnalysis.class);
        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(ScoreAnalysis.Map.class);
        job.setReducerClass(ScoreAnalysis.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(avgClassPath));

        // 统计全部班级的成绩信息
        Job joball = new Job(conf, "ScoreAnalysis");
        joball.setJarByClass(ScoreAnalysis.class);
        // 设置Map、Combine和Reduce处理类
        joball.setMapperClass(ScoreAnalysis.AllMap.class);
        joball.setReducerClass(ScoreAnalysis.AllReduce.class);
        joball.setMapOutputKeyClass(IntWritable.class);
        joball.setMapOutputValueClass(Text.class);
        joball.setOutputKeyClass(Text.class);
        joball.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(joball, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(joball, new Path(avgAllPath));

        // 做先后控制，job1结束后等待job2的结束
        if (job.waitForCompletion(true)) {
            System.exit(joball.waitForCompletion(true) ? 0 : 1);
        }
//        System.exit(joball.waitForCompletion(true) ? 0 : 1);
    }

}
