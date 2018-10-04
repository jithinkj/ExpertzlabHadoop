package DepartmentCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DCMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text k1;
    int v1;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str=value.toString().trim();
        String[] strings=str.split(",");
        k1=new Text(strings[2]);
        v1=Integer.parseInt(strings[3]);
        Text key1=new Text(strings[2]);
        int sal=Integer.parseInt(strings[3]);
        context.write(key1,new IntWritable(sal));

    }
}
