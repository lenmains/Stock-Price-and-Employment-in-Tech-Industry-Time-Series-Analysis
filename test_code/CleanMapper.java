import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CleanMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] array = line.split(",");
        if (array.length > 0 && array[0].equals("Date")) {
            return;
        }
        String date = array[0];
        if (date.substring(6,10).equals("2022")) {
            Double stock_price = Double.parseDouble(array[1].substring(1, array[1].length()));
            context.write(new Text(array[0]), new Text(stock_price));
        }
    }
}