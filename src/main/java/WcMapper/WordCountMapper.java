package WcMapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
    Text word;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String str=value.toString().trim();
        String[] words=str.split(" ");
        word=new Text(words[1]);
        for (String obj:words
             ) {
            context.write(new Text(obj),new IntWritable(1));
        }
    }
}
