package DepartmentCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class DCReducerTest {
ArrayList<IntWritable> al=new ArrayList<IntWritable>();
    @Test
    public void reduce() throws IOException, InterruptedException {
        al.add(new IntWritable(10000));
        al.add(new IntWritable(20000));
        Reducer.Context c=mock(Reducer.Context.class);
        Text key=new Text("CSE");
        Iterable<IntWritable> values=new Iterable<IntWritable>() {
            public Iterator<IntWritable> iterator() {
                return al.iterator();
            }
        };
        DCReducer reducer=new DCReducer();
        reducer.reduce(key,values,c);
        assertEquals(new IntWritable(15000),reducer.v1);
    }
}