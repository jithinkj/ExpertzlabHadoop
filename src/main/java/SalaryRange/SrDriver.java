package SalaryRange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SrDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inPath=new Path(args[0]);
        Path outPath=new Path(args[1]);
        Configuration config=new Configuration();
        Job job=new Job(config);

        job.setMapperClass(SrMapper.class);
        job.setReducerClass(SrReducer.class);
        job.setJarByClass(SrDriver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        if(job.waitForCompletion(true)){
            System.out.println("Completed Successfully");
        }
    }
}
