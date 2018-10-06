package ElectricalConsumptionProblem;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EcMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString().trim();
        String[] words=line.replaceAll("\\s+",",").split(",");

        if(!words[0].equals("Jan")){
          Text k=new Text(words[0]);
            for (int i=1;i<=words.length;i++){
                int v=Integer.parseInt(words[i]);
                context.write(k,new FloatWritable(v));
            }
        }
    }
}
