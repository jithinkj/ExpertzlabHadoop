package Students;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Path inPath=new Path(args[0]);
        Path outPath=new Path(args[1]);
        Configuration config=new Configuration();
        Job job=new Job(config);
//        job.setMapperClass(SMapper.class);
//        job.setReducerClass(SReducer.class);

        job.setMapperClass(SMapperQ67.class);
        job.setReducerClass(SReducerD67.class);

        job.setJarByClass(SDriver.class);
        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(FloatWritable.class);

        job.setMapOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        if (job.waitForCompletion(true)){
            System.out.println("Completed");
        }
    }
}
