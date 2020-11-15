package hdfsOperator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @author ywq
 * @date 2020/11/15 14:11
 */
public class hdfsTest {
    FileSystem fs=null;
    @Before
    public void init() throws Exception {
        Configuration conf=new Configuration();
        conf.set("dfs.replication","2");
        conf.set("dfs.blocksize","64m");
        fs=FileSystem.get(new URI("hdfs://hdp-01:9000"),conf,"root");
    }

    //读取hdfs文件中的内容
    @Test
    public void testRead() throws Exception
    {
        FSDataInputStream in=fs.open(new Path("/mymapreduce4/in/goods_click"));
        BufferedReader br=new BufferedReader(new InputStreamReader(in,"utf-8"));

        String line=null;
        while((line=br.readLine())!=null)
        {
            System.out.println(line);
        }
        br.close();
        in.close();
        fs.close();
    }

    //向本地磁盘向hdfs写内容
    @Test
    public void testWrite() throws Exception
    {

        FSDataOutputStream out=fs.create(new Path("input/20201115.jpg"));
        //读入本地磁盘的内容
        FileInputStream in = new FileInputStream("F:/Fairy/c.jpg");

        byte[] buf=new byte[1024];
        int read=0;
        while((read=in.read(buf))!=-1)
        {
            out.write(buf,0,read);
        }
         in.close();
        out.close();
        fs.close();
    }



}
