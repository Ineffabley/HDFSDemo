package MapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author ywq
 * @date 2020/11/15 14:46
 */
public class Temperature {
    /**
     * 四个泛型类型分别代表：
     * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
     * ValueIn      Mapper的输入数据的Value，这里是每行文字
     * KeyOut       Mapper的输出数据的Key，这里是每行文字中的“年份”
     * ValueOut     Mapper的输出数据的Value，这里是每行文字中的“气温”
     */
    static class TempMapper extends Mapper<LongWritable,Text,Text,IntWritable>
    {
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            // 打印样本: Before Mapper: 0, 2000010115

            System.out.println("Before Mapper:"+key+","+value);
            String line=value.toString();  //将每行文字转化为字符串
            String year=line.substring(0,4);
            //从第八位到最后一位就是温度
            int temperature=Integer.parseInt(line.substring(8));
            //写入年份和温度
            context.write(new Text(year), new IntWritable(temperature));
            System.out.println("After Mapper:"+new Text(year)+"---"+new IntWritable(temperature));
        }
    }

    /**
     * 求最高气温
     * 四个泛型类型分别代表：
     * KeyIn        Reducer的输入数据的Key，这里是每行文字中的“年份”
     * ValueIn      Reducer的输入数据的Value，这里是每行文字中的“气温”
     * KeyOut       Reducer的输出数据的Key，这里是不重复的“年份”
     * ValueOut     Reducer的输出数据的Value，这里是这一年中的“最高气温”
     */

    //年份要去重
    static class TempReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int maxValue=Integer.MIN_VALUE;
          StringBuffer sb=new StringBuffer();
          //取values的最大值
          for(IntWritable value:values)
          {
              maxValue=Math.max(maxValue,value.get());
              sb.append(value).append(",");
          }
            System.out.print("Before Reduce: " + key + ", " + sb.toString());
           context.write(key,new IntWritable(maxValue));  //年份和最高气温
            System.out.println(
                    "======" +
                            "After Reduce: " + key + ", " + maxValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //输入路径
        String dst="hdfs://hdp-01:9000/intput.txt";
        String out="hdfs://hdp-01:9000/output1544";

        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://hdp-01:9000");
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        Job job=new Job(conf);

        //job执行作业时输入和输出文件的路径
        FileInputFormat.addInputPath(job,new Path(dst));
        FileOutputFormat.setOutputPath(job,new Path(out));

        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        //设置最后输出结果的Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
        System.out.println("Finished");

    }


}
