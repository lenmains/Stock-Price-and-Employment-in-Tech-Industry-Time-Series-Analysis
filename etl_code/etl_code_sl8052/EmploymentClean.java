import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class EmploymentClean {

    public static void main(String[] args) throws Exception{
        if(args.length!=2){
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(EmploymentClean.class);
        job.setJobName("EmploymentClean");
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(EmploymentCleanMapper.class);
        job.setReducerClass(EmploymentCleanReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static class EmploymentCleanMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
    	    if (!line.contains("Professional, Scientific, and Technical Services")) {
    		  return;
    	    }
                
            String[] array = line.split(",");
            
    	    StringBuilder sb = new StringBuilder();
            sb.append(array[2]).append(",").append(array[array.length - 2]).append(array[array.length - 1]);
            String output = sb.toString().replace("\"", "");
            context.write(new Text(output), new Text(output));
        }
    }
    
    static class EmploymentCleanReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, null);
            }
        }
    }
}


