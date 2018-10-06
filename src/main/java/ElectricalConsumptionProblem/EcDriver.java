package ElectricalConsumptionProblem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class EcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inPath=new Path(args[0]);
        Path outPath=new Path(args[1]);
        Configuration configuration=new Configuration();
        Job job=new Job(configuration);

        job.setMapperClass(EcMapper.class);
        job.setReducerClass(EcReducer.class);
        job.setJarByClass(EcDriver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        if(job.waitForCompletion(true)){
            System.out.println("Completed Successfully");
        }
    }
}
