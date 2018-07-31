package task1.group2.q4;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import task1.GlobalConfig;

import java.io.IOException;


public class SrcDestArrDelayMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private static final int DEST_COL = GlobalConfig.DEST_COL;
    private static final int ORIGIN_COL = GlobalConfig.ORIGIN_COL;
    private static final int ARRIVAL_DELAY_COL =  GlobalConfig.ARRDELAY_COL;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) return;
        String[] tokens = value.toString().split(",");
        if (tokens.length <= GlobalConfig.ARRDELAY_COL) return;
        String origin = tokens[ORIGIN_COL];
        String dest =  tokens[DEST_COL];
        String arr_delay = tokens[ARRIVAL_DELAY_COL];
        if (origin.length() == 0 || dest.length() == 0 || arr_delay.length() == 0) return;
        String origin_carrier = origin + "_" + dest;
        context.write(new Text(origin_carrier), new FloatWritable(Float.parseFloat(arr_delay)));

    }
}
