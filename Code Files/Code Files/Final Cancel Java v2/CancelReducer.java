import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

public class CancelReducer extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

    private final Map<String,Integer> counts = new HashMap<>();
    private OutputCollector<Text, IntWritable> collector;
    @Override
    public void reduce(Text key, Iterator<IntWritable> vals,
                       OutputCollector<Text, IntWritable> output, Reporter rep)
            throws IOException {
        int sum = 0;
        while (vals.hasNext()) sum += vals.next().get();
        counts.put(key.toString(), sum);

        // Save collector for use in close()
        this.collector = output;
    }

    @Override
    public void close() throws IOException {
        if (counts.isEmpty()) {
            collector.collect(new Text("No reasons given"), new IntWritable(0));
            return;
        }

        String top1 = null, top2 = null;
        int max1 = 0, max2 = 0;

        // Find top1 and top2
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            String reason = entry.getKey();
            int count = entry.getValue();

            if (count > max1) {
                max2 = max1;
                top2 = top1;
                max1 = count;
                top1 = reason;
            } else if (count > max2) {
                max2 = count;
                top2 = reason;
            }
        }

        // If top reason is NA and there is a second reason, pick second-best
        if ("NA".equals(top1) && top2 != null) {
            collector.collect(new Text(top2), new IntWritable(max2));
        } else {
            collector.collect(new Text(top1), new IntWritable(max1));
        }
    }


}
