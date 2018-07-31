package task1.group2.q4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


public class SrcDstArrDelay {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job_1 = Job.getInstance(conf, "Source Destination Array Delay");
        job_1.setJarByClass(SrcDstArrDelay.class);
        job_1.setMapperClass(SrcDestArrDelayMapper.class);
        job_1.setReducerClass(SrcDestArrDelayReducer.class);
        job_1.setOutputKeyClass(Text.class);
        job_1.setOutputValueClass(FloatWritable.class);
        Path input_path = new Path(args[0]);
        FileInputFormat.addInputPath(job_1, input_path);
        job_1.setOutputFormatClass(NullOutputFormat.class);
        job_1.waitForCompletion(true);

        System.exit(job_1.waitForCompletion(true) ? 0 : 1);
    }
}
