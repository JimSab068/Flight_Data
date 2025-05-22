import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.*;

public class PunctualityReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
    // Map to store airline statistics
    private Map<String, double[]> airlineStats = new HashMap<>();
    private OutputCollector<Text, Text> outputCollector;

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        // Store the output collector for later use in close
        this.outputCollector = output;

        int totalFlights = 0;
        int onTimeFlights = 0;

        // Calculate on-time statistics for each airline
        while (values.hasNext()) {
            int value = values.next().get();
            totalFlights++;
            if (value == 1) {
                onTimeFlights++;
            }
        }

        double onTimeProbability = 0;
        if (totalFlights > 0) {
            onTimeProbability = (double) onTimeFlights / totalFlights;
        }

        // Store airline statistics for later sorting
        airlineStats.put(key.toString(), new double[] {onTimeProbability, totalFlights});

        // Do not write anything here - we'll only write the top and bottom airlines in close
    }

    @Override
    public void close() throws IOException {
        // Create a list of airline entries sorted by on-time probability
        List<Map.Entry<String, double[]>> sortedAirlines = new ArrayList<>(airlineStats.entrySet());
        sortedAirlines.sort((entry1, entry2) -> Double.compare(entry2.getValue()[0], entry1.getValue()[0]));

        // Print top 3 airlines with highest on-time probabilities
        outputCollector.collect(new Text("Top 3 Airlines with Highest On-Time Probability:"), new Text(""));
        for (int i = 0; i < 3 && i < sortedAirlines.size(); i++) {
            Map.Entry<String, double[]> entry = sortedAirlines.get(i);
            String airline = entry.getKey();
            double onTimeProbability = entry.getValue()[0];
            outputCollector.collect(
                new Text("Airline: " + airline),
                new Text(" | On-Time Probability: " + onTimeProbability)
            );
        }

        // Print bottom 3 airlines with lowest on-time probabilities
        outputCollector.collect(new Text("\nBottom 3 Airlines with Lowest On-Time Probability:"), new Text(""));
        for (int i = sortedAirlines.size() - 1; i >= sortedAirlines.size() - 3 && i >= 0; i--) {
            Map.Entry<String, double[]> entry = sortedAirlines.get(i);
            String airline = entry.getKey();
            double onTimeProbability = entry.getValue()[0];
            outputCollector.collect(
                new Text("Airline: " + airline),
                new Text(" | On-Time Probability: " + onTimeProbability)
            );
        }
    }
}
