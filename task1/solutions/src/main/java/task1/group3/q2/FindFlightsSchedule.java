package task1.group3.q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import task1.group2.TextArrayWritable;
import task1.group2.q2.SrcDestMapper;
import task1.group2.q2.SrcDestReducer;
import task1.group2.q2.Top10DestMapper;
import task1.group2.q2.Top10DestReducer;


public class FindFlightsSchedule {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path tmp_path = new Path("/ccc-tmp/task1-3-q2-tmp");
        Path tmp_path2 = new Path("/tmp/ccc/task1-3-q2-tmp2");

        Job job_1 = Job.getInstance(conf, "flight xyz");
        job_1.setJarByClass(FindFlightsSchedule.class);

        job_1.setMapperClass(FlightXYZMapper.class);
        job_1.setReducerClass(FlightXYZReducer.class);

        job_1.setMapOutputKeyClass(Text.class);
        job_1.setMapOutputValueClass(FlightRecord.class);

        job_1.setOutputKeyClass(NullWritable.class);
        job_1.setOutputValueClass(FlightRecord.class);

        Path input_path = new Path(args[0]);
        FileInputFormat.addInputPath(job_1, input_path);
        FileOutputFormat.setOutputPath(job_1, tmp_path);
        job_1.waitForCompletion(true);

        Job job_2 = Job.getInstance(conf, "flight schedule");
        job_2.setJarByClass(FindFlightsSchedule.class);
        job_2.setMapperClass(FlightSchedulerMapper.class);
        job_2.setReducerClass(FlightSchedulerReducer.class);

        job_2.setMapOutputKeyClass(Text.class);
        job_2.setMapOutputValueClass(FlightRecord.class);

        job_2.setOutputValueClass(Text.class);
        job_2.setOutputFormatClass(NullOutputFormat.class);

        FileInputFormat.addInputPath(job_2, tmp_path);

        System.exit(job_2.waitForCompletion(true) ? 0 : 1);

        job_2.setOutputFormatClass(NullOutputFormat.class);
    }
}
