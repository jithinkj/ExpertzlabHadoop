package Students;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SReducerD67 extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String line=new String();
        int sum=0;
        for (Text name:values
             ) {
            line+=name+" ";
            String x = name.toString().replaceAll(".*\\(|\\).*", "");
            sum+=Integer.parseInt(x);
        }
//        context.write(key,new Text(line));

//      *****************Qsn 7*********************************
        line+="--"+Integer.toString(sum);
        context.write(key,new Text(line));
//        ******************************************************




    }
}
