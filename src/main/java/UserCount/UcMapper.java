package UserCount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UcMapper extends Mapper<LongWritable,Text,IntWritable, FloatWritable> {
    IntWritable k;
    FloatWritable v;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines=value.toString().trim();

        String line=lines.replaceAll("\\s+","-");
        String[] words=line.split("-");


        if(words.length>2){
            k=new IntWritable(Integer.parseInt(words[1]));
            v=new FloatWritable(Integer.parseInt(words[2]));
            int m_id=Integer.parseInt(words[1]);
            int r=Integer.parseInt(words[2]);
            context.write(new IntWritable(m_id),new FloatWritable(r));
        }
    }
}
