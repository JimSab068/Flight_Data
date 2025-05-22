import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class PunctualityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        // Skip header line if present (first line)
        String line = value.toString();
        if (line.startsWith("Year") || line.contains("Year,Month")) {
            return;
        }

        // Split the line into fields
        String[] fields = line.split(",");
        if (fields.length < 16) {
            return; // Skip if the line doesn't have enough columns
        }

        String airline = fields[8];  // Airline code
        String onTimeStatus = fields[15];  // On-time status

        try {
            int onTime = Integer.parseInt(onTimeStatus);
            // Output: key=airline, value=1 if on-time, 0 if not
            if (onTime < 10) {
                output.collect(new Text(airline), one);  // On-time
            } else {
                output.collect(new Text(airline), zero);  // Not on-time
            }
        } catch (NumberFormatException e) {
            // Skip invalid entries without any output
            reporter.incrCounter("PunctualityMapper", "InvalidEntries", 1);
        }
    }
}