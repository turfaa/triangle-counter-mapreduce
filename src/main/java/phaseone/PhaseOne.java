package phaseone;

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
import java.util.ArrayList;
import java.util.List;

public class PhaseOne {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "phase one ku");

        job.setJarByClass(PhaseOne.class);
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
            context.write(user, follower);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        Text child = new Text();

        @Override
        protected void reduce(Text user, Iterable<Text> neighbors, Context context) throws IOException, InterruptedException {
            List<String> neighbor_list = new ArrayList<>();

            for (Text neighbor : neighbors) {
                neighbor_list.add(neighbor.toString());
            }
            for (int u = 0; u < neighbor_list.size(); u++) {
                for (int v = u+1; v < neighbor_list.size(); v++) {
                    if (neighbor_list.get(u).compareTo(neighbor_list.get(v)) < 0) {
                        child.set(neighbor_list.get(u) + ',' + neighbor_list.get(v));
                    }
                    else {
                        child.set(neighbor_list.get(v) + ',' + neighbor_list.get(u));
                    }

                    context.write(user, child);
                }
            }
        }
    }
}
