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

public class Transform {

    public static void main(String[] args) throws Exception{
        if(args.length!=2){
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Transform.class);
        job.setJobName("Stats");
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TransformMapper.class);
        job.setReducerClass(TransformReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static class TransformMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] array = line.split(",");
            if (array.length > 0 && array[0].equals("Date")) {
                return;
            }
            Integer month = Integer.parseInt(array[0].substring(0, 2));
            Integer date = Integer.parseInt(array[0].substring(3, 5));
            if (month > 12 || date > 31) {
                return;
            }
            array[1] = array[1].substring(1);
            array[3] = array[3].substring(1);
            array[4] = array[4].substring(1);
            array[5] = array[5].substring(1);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < array.length; i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(array[i]);
            }
            context.write(new Text(sb.toString()), new Text(sb.toString()));
        }
    }
    static class TransformReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, null);
            }
        }
    }
}


