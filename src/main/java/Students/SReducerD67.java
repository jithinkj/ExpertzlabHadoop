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
        for (Text name:values
             ) {
            line+=name+" ";
        }
//        context.write(key,new Text(line));

//      *****************Qsn 7*********************************
        List<String> matchList = new ArrayList<String>();
        Pattern regex = Pattern.compile("\\((.*?)\\)");
        Matcher regexMatcher = regex.matcher(line);

        while (regexMatcher.find()) {//Finds Matching Pattern in String
            matchList.add(regexMatcher.group(1));//Fetching Group from String
        }
        int mark=0;
        for(String str:matchList) {
            mark+=Integer.parseInt(str);
        }
        line+="--"+Integer.toString(mark);
        context.write(key,new Text(line));
//        ******************************************************
    }
}
