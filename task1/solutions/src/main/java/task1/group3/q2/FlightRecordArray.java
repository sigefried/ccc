package task1.group3.q2;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class FlightRecordArray extends ArrayWritable {
    public FlightRecordArray() {
        super(FlightRecord.class);
    }

    public FlightRecordArray(FlightRecord[] datas) {
        super(FlightRecord.class);
        set(datas);
    }
}

