package SalaryRange;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SrMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str=value.toString().trim();
        String[] words=str.split(",");
        String catA="<=10000";
        String catB="<=15000";
        String catC="<=100000";
        int sal=Integer.parseInt(words[3]);
        Text name=new Text(words[1]);
        if (sal<=10000){
            context.write(new Text(catA),name);
        }
        if (sal>10000 && sal<=15000){
            context.write(new Text(catB),name);
        }
        if (sal>15000 &&sal<=100000){
            context.write(new Text(catC),name);
        }

    }
}
