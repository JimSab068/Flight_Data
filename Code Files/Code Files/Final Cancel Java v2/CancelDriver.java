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

public class CancelDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inputDir = conf.get("mapred.input.dir");
        String outputDir = conf.get("mapred.output.dir");
        int fileLimit = conf.getInt("file.limit", 1); // default to 1

        JobConf jobConf = new JobConf(conf, CancelDriver.class);
        jobConf.setJobName("Most Common Cancellation Reason");

        jobConf.setMapperClass(CancelMapper.class);
        jobConf.setReducerClass(CancelReducer.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(inputDir));
        Arrays.sort(files, Comparator.comparing(f -> f.getPath().getName()));

        int count = 0;
        for (FileStatus file : files) {
            if (!file.isFile()) continue;
            FileInputFormat.addInputPath(jobConf, file.getPath());
            count++;
            if (count >= fileLimit) break;
        }

        FileOutputFormat.setOutputPath(jobConf, new Path(outputDir));

        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
CancelDriver driver = new CancelDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);

    }
}
