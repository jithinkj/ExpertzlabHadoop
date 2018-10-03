package UserCount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class UcMapperTest {

    @Test
    public void map() throws IOException, InterruptedException {
        Mapper.Context c=mock(Mapper.Context.class);
        LongWritable key=new LongWritable(100);
        Text value=new Text("186 242 3 1058764");
        UcMapper ucMapper=new UcMapper();
        ucMapper.map(key,value,c);
        assertEquals(new IntWritable(242),ucMapper.k);
        assertEquals(new FloatWritable(3),ucMapper.v);
    }
}