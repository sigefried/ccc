package task1.group2.q1;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import task1.group2.TextArrayWritable;

import java.io.IOException;


public class Top10CarrierMapper extends Mapper<Object, Text, Text, TextArrayWritable> {

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(";");
        if (tokens.length < 2) return;
        String airport_carrier = tokens[0];
        String avg_depdelay = tokens[1];
        String[] sub_tokens = airport_carrier.split("_");
        String origin = sub_tokens[0];
        String carrier = sub_tokens[1];

        context.write(new Text(origin), new TextArrayWritable(new String[]{carrier, avg_depdelay}));

    }
}
