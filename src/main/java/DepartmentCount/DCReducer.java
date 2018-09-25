package DepartmentCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DCReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        int count=0;
        for (IntWritable val:values
             ) {
            sum+=val.get();
            count++;
        }
        int avg=sum/count;
        context.write(key,new IntWritable(avg));
    }
}
