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

public class Stats {

    public static void main(String[] args) throws Exception{
        if(args.length!=2){
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(Stats.class);
        job.setJobName("Stats");
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
            context.write(new Text("data"), new DoubleWritable(Double.parseDouble(list[1])));
        }
    }
    static class StatsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            Double sum = 0d;
            List<Double> list = new ArrayList<>();
            Map<Double, Integer> map = new HashMap<>();
            for (DoubleWritable value : values) {
                cnt++;
                sum += value.get();
                list.add(value.get());
                map.put(value.get(), map.getOrDefault(value, 0) + 1);
            }
            Double mean = sum / cnt;
            Double median;
            Integer mode = 0;
            Collections.sort(list);
            if (cnt % 2 == 1) {
                median = list.get(cnt / 2);
            } else {
                median = (list.get(cnt / 2 - 1) + list.get(cnt / 2)) / 2;
            }

            for (Map.Entry<Double, Integer> entry : map.entrySet()) {
                mode = Math.max(mode, entry.getValue());
            }

            context.write(new Text("mean: " + mean), null);
            context.write(new Text("median: " + median), null);
            context.write(new Text("mode: " + mode), null);
        }
    }
}


