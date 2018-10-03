package WcMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        Configuration config=new Configuration();
        Job job=new Job(config);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setJarByClass(WcDriver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);



        //to check output correctness

        if(job.waitForCompletion(true)){
            System.out.println("Completed successfully");
        }
    }
}
