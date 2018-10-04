package WcMapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class WordCountReducerTest {
    //to convert to iterator we have to create an array list
    ArrayList<IntWritable> al=new ArrayList<IntWritable>();

    @Test
    public void reduce() throws IOException, InterruptedException {
        al.add(new IntWritable(1)); // define arraylist
        al.add(new IntWritable(1));
        al.add(new IntWritable(1));

        Reducer.Context c=mock(Reducer.Context.class);
        Text key=new Text("hai");
        Iterable<IntWritable> values=new Iterable<IntWritable>() {
            public Iterator<IntWritable> iterator() {
                return al.iterator(); //change return to al.iterator
            }
        };
        WordCountReducer wordCountReducer=new WordCountReducer();
        wordCountReducer.reduce(key,values,c);
        assertEquals(3,wordCountReducer.count);
    }
}