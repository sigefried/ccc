package task1.group1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import task1.GlobalConfig;

import javax.xml.bind.annotation.XmlElementDecl;
import java.io.IOException;

public class AirLineDelay {

    public static class AirLineDelayMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;
            String[] line = value.toString().split(",");
            if (line.length <= GlobalConfig.ARRDELAY_COL) return;
            String air_line = line[GlobalConfig.UNIQUECARRIER_COL];
            String arr_delay = line[GlobalConfig.ARRDELAY_COL];
            if (air_line.length() == 0 || arr_delay.length() == 0) return;
            context.write(new Text(air_line), new FloatWritable(Float.parseFloat(arr_delay)));
        }
    }

    public static class AirLineDelayReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count += 1;
            }
            context.write(key, new FloatWritable(sum / count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AirLineDelay");
        job.setJarByClass(AirLineDelay.class);
        job.setMapperClass(AirLineDelayMapper.class);
        job.setReducerClass(AirLineDelayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        Path input_path = new Path(args[0]);
        Path output_path = new Path(args[1]);
        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}