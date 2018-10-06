package ElectricalConsumptionProblem;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EcReducer extends Reducer<Text, FloatWritable,Text,FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum= 0,count=0,avg=0;
        for (FloatWritable v:values
             ) {
            sum+=v.get();
            count++;
        }
        avg=sum/count;
        context.write(key,new FloatWritable(avg));
    }
}
