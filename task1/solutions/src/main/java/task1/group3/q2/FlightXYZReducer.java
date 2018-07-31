package task1.group3.q2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlightXYZReducer extends Reducer<Text, FlightRecord, NullWritable, FlightRecord> {


    @Override
    public void reduce(Text key, Iterable<FlightRecord> values, Context context) throws IOException, InterruptedException {

        float minDelay = Float.MAX_VALUE;
        FlightRecord bestRecord = null;
        for (FlightRecord flightRecord : values) {
            float arrDelay = flightRecord.getFlightArrDelay().get();
            if (arrDelay < minDelay) {
                minDelay = arrDelay;
                bestRecord = new FlightRecord(flightRecord);
            }
        }
        context.write(NullWritable.get(), bestRecord);
    }

}
