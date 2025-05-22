import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaxiTimeCalculator extends Configured implements Tool {

    // Mapper class using old API
    public static class TaxiMapper extends MapReduceBase implements Mapper<Object, Text, Text, FloatWritable> {

        private Text pair = new Text();
        private FloatWritable taxiTime = new FloatWritable();
        private int invalidRecords = 0; // Counter for invalid records

        @Override
        public void map(Object key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();

            // Skip header line
            if (line.startsWith("Year") || line.contains("Origin") || line.contains("TaxiIn")) {
                return;
            }

            String[] fields = line.split(",");

            // Check that the fields are valid and not "NA"
            if (fields.length > 21) {
                try {
                    String origin = fields[16].trim();
                    String dest = fields[17].trim();
                    String taxiOutStr = fields[20].trim();
                    String taxiInStr = fields[19].trim();

                    // Check if any field is empty or "NA", or if origin equals destination
                    if (origin.isEmpty() || dest.isEmpty() || origin.equalsIgnoreCase("NA") || dest.equalsIgnoreCase("NA") ||
                        taxiOutStr.isEmpty() || taxiInStr.isEmpty() || taxiOutStr.equalsIgnoreCase("NA") || taxiInStr.equalsIgnoreCase("NA") ||
                        origin.equals(dest)) {

                        // Print message for invalid airports
                        System.out.println("No valid airports are available for line: " + line);
                        invalidRecords++;
                        reporter.incrCounter("TaxiTimeCalculator", "InvalidRecords", 1);
                        return;
                    }

                    float taxiOut = Float.parseFloat(taxiOutStr);
                    float taxiIn = Float.parseFloat(taxiInStr);
                    float totalTaxiTime = taxiOut + taxiIn;

                    // Create the key as "origin-destination"
                    String keyString = origin + "-" + dest;
                    pair.set(keyString);

                    // Calculate the total taxi time (out + in) and set it as the value
                    taxiTime.set(totalTaxiTime);
                    output.collect(pair, taxiTime);

                    // Also emit entries for individual airports for origin and destination
                    // Origin airport - taxi out time
                    pair.set("ORIGIN:" + origin);
                    taxiTime.set(totalTaxiTime);
                    output.collect(pair, taxiTime);

                    // Destination airport - taxi in time
                    pair.set("DEST:" + dest);
                    taxiTime.set(totalTaxiTime);
                    output.collect(pair, taxiTime);

                } catch (NumberFormatException e) {
                    // Print message for invalid number format
                    System.out.println("No valid airports are available due to number format exception: " + line);
                    invalidRecords++;
                    reporter.incrCounter("TaxiTimeCalculator", "NumberFormatErrors", 1);
                }
            } else {
                // Print message for invalid field count
                System.out.println("No valid airports are available due to insufficient fields: " + line);
                invalidRecords++;
                reporter.incrCounter("TaxiTimeCalculator", "InsufficientFields", 1);
            }
        }

        @Override
        public void close() throws IOException {
            // Report the total number of invalid records
            System.out.println("Total records with no valid airports available: " + invalidRecords);
            super.close();
        }
    }

    // Modified Reducer class - only outputs summary information
    public static class TaxiReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();
        private Map<String, Float> airportPairAvgs = new HashMap<>();
        private Map<String, Float> airportAvgTaxiTimes = new HashMap<>();

        // Variables to track global min/max
        private String globalMinKey = null;
        private String globalMaxKey = null;
        private float globalMinAvg = Float.MAX_VALUE;
        private float globalMaxAvg = Float.MIN_VALUE;
        private OutputCollector<Text, FloatWritable> outputCollector;

        @Override
        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            // Store the output collector for use in close()
            if (outputCollector == null) {
                outputCollector = output;
            }

            float sum = 0;
            int count = 0;

            // Iterate over the values and calculate the sum and count
            while (values.hasNext()) {
                sum += values.next().get();
                count++;
            }

            // Calculate the average taxi time
            float avg = sum / count;
            result.set(avg);

            String keyStr = key.toString();

            // Handle airport pair data
            if (!keyStr.startsWith("ORIGIN:") && !keyStr.startsWith("DEST:")) {
                // Store the key-value pair in our map (but don't output it)
                airportPairAvgs.put(keyStr, avg);

                // Track global min/max for airport pairs
                if (avg < globalMinAvg) {
                    globalMinAvg = avg;
                    globalMinKey = keyStr;
                }
                if (avg > globalMaxAvg) {
                    globalMaxAvg = avg;
                    globalMaxKey = keyStr;
                }
            }
            // Handle individual airport data
            else if (keyStr.startsWith("ORIGIN:") || keyStr.startsWith("DEST:")) {
                String airportCode = keyStr.substring(keyStr.indexOf(":") + 1);

                // Store airport data without outputting it
                if (!airportAvgTaxiTimes.containsKey(airportCode)) {
                    airportAvgTaxiTimes.put(airportCode, avg);
                } else {
                    // Update the average (simple average of the averages for simplicity)
                    float currentAvg = airportAvgTaxiTimes.get(airportCode);
                    float newAvg = (currentAvg + avg) / 2;
                    airportAvgTaxiTimes.put(airportCode, newAvg);
                }
            }
            // No output.collect calls here - we're just accumulating data
        }

        @Override
        public void close() throws IOException {
            if (outputCollector == null) {
                System.err.println("Output collector was not initialized in reduce()");
                return;
            }

            // Sort airports by avg taxi time
            List<Map.Entry<String, Float>> airportList = new ArrayList<>(airportAvgTaxiTimes.entrySet());
            Collections.sort(airportList, (a, b) -> Float.compare(a.getValue(), b.getValue()));

            if (airportList.isEmpty()) {
                // Output a message if there are no valid airports
                result.set(0);
                outputCollector.collect(new Text("No valid airports are available."), result);
            } else {
                // Output airport pair with longest/shortest taxi times
                result.set(globalMaxAvg);
                outputCollector.collect(new Text("MAX_PAIR:" + globalMaxKey), result);

                result.set(globalMinAvg);
                outputCollector.collect(new Text("MIN_PAIR:" + globalMinKey), result);

                // Output top 3 airports with longest taxi times
                int size = airportList.size();
                outputCollector.collect(new Text("TOP_LONGEST:"), new FloatWritable(0));
                for (int i = 0; i < 3 && i < size; i++) {
                    Map.Entry<String, Float> entry = airportList.get(size - 1 - i);
                    result.set(entry.getValue());
                    outputCollector.collect(new Text("TOP_LONGEST:" + (i+1) + ":" + entry.getKey()), result);
                }

                // Output top 3 airports with shortest taxi times
                outputCollector.collect(new Text("TOP_SHORTEST:"), new FloatWritable(0));
                for (int i = 0; i < 3 && i < size; i++) {
                    Map.Entry<String, Float> entry = airportList.get(i);
                    result.set(entry.getValue());
                    outputCollector.collect(new Text("TOP_SHORTEST:" + (i+1) + ":" + entry.getKey()), result);
                }
            }

            super.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // Get parameters from configuration or arguments
        Configuration conf = getConf();
        String inputDir = args.length > 0 ? args[0] : conf.get("mapred.input.dir");
        String outputPath = args.length > 1 ? args[1] : conf.get("mapred.output.dir");

        // Get numYears from configuration if not provided as argument
        int numYears;
        if (args.length > 2) {
            numYears = Integer.parseInt(args[2]);
        } else {
            numYears = conf.getInt("num.years", 1); // Default to 1 if not specified
        }

        // Use JobConf for old API compatibility
        JobConf jobConf = new JobConf(conf, TaxiTimeCalculator.class);
        jobConf.setJobName("Taxi Time Calculator");

        // Set input and output formats (old API)
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        // Set Mapper and Reducer classes
        jobConf.setMapperClass(TaxiMapper.class);
        jobConf.setReducerClass(TaxiReducer.class);

        // Set output types
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(FloatWritable.class);

        // Dynamically add only the first N files
        FileSystem fs = FileSystem.get(jobConf);
        FileStatus[] files = fs.listStatus(new Path(inputDir), path -> path.getName().endsWith(".csv"));

        // Sort files by name
        Arrays.sort(files, Comparator.comparing(f -> f.getPath().getName()));

        if (numYears > files.length) {
            System.err.println("Requested more years than available files: " + numYears + " > " + files.length);
            return -1;
        }

        // Add input paths using old API
        for (int i = 0; i < numYears; i++) {
            FileInputFormat.addInputPath(jobConf, files[i].getPath());
            System.out.println("Added input file: " + files[i].getPath().getName());
        }

        // Set output path using old API
        Path outputDir = new Path(outputPath);
        FileOutputFormat.setOutputPath(jobConf, outputDir);

        // Run the job using old API
        JobClient.runJob(jobConf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new TaxiTimeCalculator(), args);
        System.exit(exitCode);
    }
}