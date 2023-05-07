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

public class StatsSpecificYear {

    public static void main(String[] args) throws Exception{
        if(args.length!=2){
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(StatsSpecificYear.class);
        job.setJobName("StatsSpecificYear");
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static class StatsMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] list = value.toString().split(",");
            String k = list[0].substring(6, 10) + ",";
            context.write(new Text(k), new DoubleWritable(Double.parseDouble(list[1])));
        }
    }
    static class StatsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            Double sum = 0d;
            for (DoubleWritable value : values) {
                cnt++;
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum / cnt));
        }
    }
}


