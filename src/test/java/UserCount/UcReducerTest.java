package UserCount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class UcReducerTest {
ArrayList<FloatWritable> al=new ArrayList<FloatWritable>();
    @Test
    public void reduce() throws IOException, InterruptedException {
        al.add(new FloatWritable(3));
        al.add(new FloatWritable(4));
        al.add(new FloatWritable(3));
        al.add(new FloatWritable(2));
        Reducer.Context context=mock(Reducer.Context.class);
        IntWritable key=new IntWritable(242);
        Iterable<FloatWritable> values=new Iterable<FloatWritable>() {
            public Iterator<FloatWritable> iterator() {
                return al.iterator();
            }
        };
        UcReducer reducer=new UcReducer();
        reducer.reduce(key,values,context);
        assertEquals(new FloatWritable(3),reducer.v1);
    }
}