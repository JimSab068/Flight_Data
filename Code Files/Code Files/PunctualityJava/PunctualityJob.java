import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.Comparator;

public class PunctualityJob extends Configured implements Tool {

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
        JobConf jobConf = new JobConf(conf, PunctualityJob.class);
        jobConf.setJobName("Punctuality Job");

        // Set Mapper and Reducer classes
        jobConf.setMapperClass(PunctualityMapper.class);
        jobConf.setReducerClass(PunctualityReducer.class);

        // Set output types
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

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
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));

        // Submit job using old API
        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PunctualityJob(), args);
        System.exit(exitCode);
    }
}