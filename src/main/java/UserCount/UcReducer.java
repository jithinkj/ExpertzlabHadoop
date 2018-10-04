package UserCount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UcReducer extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable> {
    FloatWritable v1;
    @Override
    protected void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        int count=0;

        for (FloatWritable k:values
        ) {
            sum+=k.get();
            count++;
        }
        float avg=sum/count;
        v1=new FloatWritable(avg);
        float avgPercentage=avg/5*100;
        context.write(key,new FloatWritable(avgPercentage));
    }
}
