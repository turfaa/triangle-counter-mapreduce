package preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Preprocessor {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "preps");

        job.setJarByClass(Preprocessor.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.submit();
    }

    public static class Map extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text user, Text follower, Context context) throws IOException, InterruptedException {
            String user_string = user.toString();
            String follower_string = follower.toString();

            if (user_string.compareTo(follower_string) < 0) {
                context.write(user, follower);
            }
            else {
                context.write(follower, user);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text user, Iterable<Text> neighbors, Context context) throws IOException, InterruptedException {
            Set<String> visited = new HashSet<>();

            for (Text neighbor : neighbors) {
                String neighbor_string = neighbor.toString();
                if (!visited.contains(neighbor_string)) {
                    context.write(user, neighbor);
                    visited.add(neighbor_string);
                }
            }
        }
    }
}
