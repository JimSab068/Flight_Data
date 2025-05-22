import java.util.List;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CancelMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text reason = new Text();
    private static final List<String> validCodes = Arrays.asList("A", "B", "C", "D","NA");


    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String[] fields = value.toString().split(",");
	// Skip the header
        if ( value.toString().contains("Year")) {
            return;
        }

        if (fields.length > 22) {
            String cancelled = fields[21].trim();
            String code = fields[22].trim();
            String year = fields[0].trim();

            if ("1".equals(cancelled) && validCodes.contains(code)) {
                reason.set(code);
                output.collect(reason, one);
            }

        }

    }
}
