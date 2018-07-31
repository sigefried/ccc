package task1.group2.q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import task1.group2.TextArrayWritable;


public class TopRankDestWithOrigin {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path tmp_path = new Path("/ccc-tmp/task1-2-q2-tmp");

        Job job_1 = Job.getInstance(conf, "Dest Performance by Origin");
        job_1.setJarByClass(TopRankDestWithOrigin.class);
        job_1.setMapperClass(SrcDestMapper.class);
        job_1.setReducerClass(SrcDestReducer.class);
        job_1.setOutputKeyClass(Text.class);
        job_1.setOutputValueClass(FloatWritable.class);
        Path input_path = new Path(args[0]);
        FileInputFormat.addInputPath(job_1, input_path);
        FileOutputFormat.setOutputPath(job_1, tmp_path);
        job_1.waitForCompletion(true);

        Job job_2 = Job.getInstance(conf, "Top Dest per Origin");
        job_2.setJarByClass(TopRankDestWithOrigin.class);

        job_2.setMapperClass(Top10DestMapper.class);
        job_2.setReducerClass(Top10DestReducer.class);
        job_2.setMapOutputKeyClass(Text.class);
        job_2.setMapOutputValueClass(TextArrayWritable.class);
        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(Text.class);
        job_2.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job_2, tmp_path);

        System.exit(job_2.waitForCompletion(true) ? 0 : 1);
    }
}
