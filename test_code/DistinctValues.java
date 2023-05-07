import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DistinctValues {

    public static void main(String[] args) throws Exception{
        if(args.length!=2){
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(DistinctValues.class);
        job.setJobName("Distinct Values");
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DistinctValuesMapper.class);
        job.setReducerClass(DistinctValuesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static class DistinctValuesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] list = value.toString().split(",");

            context.write(new Text(list[0]), new Text(list[1]));
        }
    }
    static class DistinctValuesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for (Text value : values) {
                if (cnt == 1) {
                    context.write(key, new Text("Duplicate values found"));
                }
                context.write(key, value);
            }
        }
    }
}


