package Students;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SReducer extends Reducer<Text, FloatWritable,Text,FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum=0;int count=0;
        for (FloatWritable mark:values
             ) {
            sum+=mark.get();
            count++;
        }
        float avg=sum/count;
        float percentage=sum/4;
//        **********************output 1 *******************
//        context.write(key,new FloatWritable(sum));
//        **************************************************

//        **********************output 2 *******************
//        context.write(key,new FloatWritable(avg));
//        **************************************************

//        **********************output 3 *******************
//        context.write(key,new FloatWritable(percentage));
//        **************************************************

//        **********************output 4 *******************
//        float phyM=0;
//        for (FloatWritable mark:values
//        ) {
//            phyM=mark.get();
//            if(mark.get()>50){
//                context.write(key,new FloatWritable(phyM));
//            }
//        }
//        **************************************************

//        **********************output 5 *******************
        context.write(key,new FloatWritable(count));
//        **************************************************

    }
}
