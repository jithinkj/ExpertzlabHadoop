package SalaryRange;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SrReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String names=new String();
        for (Text val:values
             ) {
          names+="-"+val.toString()+"-";
        }
        context.write(key,new Text(names));
    }
}
