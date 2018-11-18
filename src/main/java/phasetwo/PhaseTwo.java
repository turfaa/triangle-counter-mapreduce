package phasetwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class PhaseTwo {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "phase two ku");

        job.setJarByClass(PhaseTwo.class);
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
        Text two_node = new Text();
        Text dollar = new Text("$");

        @Override
        protected void map(Text u, Text v, Context context) throws IOException, InterruptedException {
            if (v.toString().contains(",")) {
                context.write(v, u);
            } else {
                two_node.set(u.toString() + ',' + v.toString());
                context.write(two_node, dollar);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
        Text three_node = new Text();
        IntWritable one = new IntWritable(1);

        @Override
        protected void reduce(Text user, Iterable<Text> neighbors, Context context) throws IOException, InterruptedException {
            List<String> neighborList = new ArrayList<>();
            boolean contain_dollar = false;

            for (Text neighbor : neighbors) {
                neighborList.add(neighbor.toString());

                if (neighbor.toString().equals("$")) {
                    contain_dollar = true;
                }
            }

            if (contain_dollar) {
                for (String neighbor : neighborList) {
                    if (!neighbor.equals("$")) {
                        three_node.set(user.toString() + ',' + neighbor);
                        context.write(three_node, one);
                    }
                }
            }
        }
    }
}
