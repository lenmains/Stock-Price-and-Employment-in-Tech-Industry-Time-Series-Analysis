
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CleanReducer
    extends Reducer<Text, Text, Text, Text> {
    private IntWritable result = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String result = key + "," + value;
            context.write(new Text(result), null);
        }
    }
}