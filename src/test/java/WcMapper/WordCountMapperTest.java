package WcMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class WordCountMapperTest {

    @Test
    public void map() throws IOException, InterruptedException {
        Mapper.Context c=mock(Mapper.Context.class);
        LongWritable key=new LongWritable(100);
        Text value=new Text("hai hello how are you");
        WordCountMapper wordCountMapper=new WordCountMapper();
        wordCountMapper.map(key,value,c);
        assertEquals(new Text("hello"),wordCountMapper.word);

    }
}