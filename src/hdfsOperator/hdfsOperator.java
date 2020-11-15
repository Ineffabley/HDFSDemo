package hdfsOperator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author ywq
 * @date 2020/11/15 10:16
 */

/*
* 从windows系统操作hdfs
* */
public class hdfsOperator {
    public static void main(String[] args) throws Exception {
        /*加载hadoop的[配置*/
        Configuration conf=new Configuration();

        conf.set("dfs.replication","2");  //指定保存的副本数
        conf.set("dfs.blocksize","64m");  //切块大小
        //构造一个访问指定hdfs系统的客户端对象
        FileSystem fs=FileSystem.get(new URI("hdfs://hdp-01:9000/"),conf,"root");
        //上传一个文件到HDFS中 ()
        fs.copyFromLocalFile(new Path("E:/20201115.txt"),new Path("/input/"));
        fs.close();
        System.out.println("上传完成");
    }

}
