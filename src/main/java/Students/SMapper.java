package Students;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SMapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString().trim();
        String[] words=line.split(" ");
        int mark=Integer.parseInt(words[3]);
//        **********************Qsn 1,2,3 *******************************************************
//        context.write(new Text(words[0]),new FloatWritable(mark));
//        *************************************************************
//        *************************Qsn 4*******************************************************
//        String subject=words[2];
//        if(subject.equals("physics")){
//            context.write(new Text(words[0]),new FloatWritable(mark));
//        }
//        *************************************************************
//        **************************Qsn 5********************************
        if(mark>50){
            context.write(new Text(words[2]),new FloatWritable(1));
        }
//        ***************************************************************
    }
}
