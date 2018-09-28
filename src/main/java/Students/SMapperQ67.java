package Students;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SMapperQ67 extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString().trim();
        String[] words=line.split(" ");
        int mark=Integer.parseInt(words[3]);
        String name=words[0];
        String subject=words[2];
        String v1=name+"("+words[3]+")";
        String v2=subject+"("+words[3]+")";


//        ***********Qsn 6**********************
//        if(mark<50){
//            context.write(new Text(subject),new Text(v1));
//        }
//        ******************************************
//        ****************************Qsn7****************************
        if(mark>50){
            context.write(new Text(name),new Text(v2));
        }
//        ***********************************************************

    }
}
