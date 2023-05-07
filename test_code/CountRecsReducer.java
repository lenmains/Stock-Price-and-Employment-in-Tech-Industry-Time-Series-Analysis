// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountRecsReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        context.write(new Text("Total number of records in the data set before cleaning: "), result);
    }
}
// ^^ MaxTemperatureReducer