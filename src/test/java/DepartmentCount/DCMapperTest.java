package DepartmentCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class DCMapperTest {

    @Test
    public void map() throws IOException, InterruptedException {
        Mapper.Context c=mock(Mapper.Context.class);
        LongWritable key=new LongWritable(1000);
        Text value=new Text("jithin,1,cse,20000");
        DCMapper dcMapper=new DCMapper();
        dcMapper.map(key,value,c);
        assertEquals(20000,dcMapper.v1);
    }
}