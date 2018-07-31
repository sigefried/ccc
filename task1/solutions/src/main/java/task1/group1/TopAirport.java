package task1.group1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import task1.GlobalConfig;

import java.io.IOException;

public class TopAirport {

    public static class TopAirportMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            String[] line = value.toString().split(",");
            if (line.length <= GlobalConfig.DEST_COL) return;
            String origin_airport = line[GlobalConfig.ORIGIN_COL];
            String dest_airport = line[GlobalConfig.DEST_COL];
            if (origin_airport.length() != 0) {
                context.write(new Text(origin_airport), one);
            }

            if (dest_airport.length() != 0) {
                context.write(new Text(dest_airport), one);
            }
        }
    }

    public static class TopAirportReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopAirport");
        job.setJarByClass(TopAirport.class);
        job.setMapperClass(TopAirportMapper.class);
        job.setCombinerClass(TopAirportReducer.class);
        job.setReducerClass(TopAirportReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path input_path = new Path(args[0]);
        Path output_path = new Path(args[1]);
        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}