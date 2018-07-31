package task1.group3.q2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import task1.GlobalConfig;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class FlightSchedulerMapper extends Mapper<LongWritable, Text, Text, FlightRecord> {
    private int ORIGIN_COL = 0;
    private int DEST_COL = 1;
    private int FLIGHT_NUM_COL = 2;
    private int DATE_COL = 3;
    private int DEPTIME_COL = 4;
    private int ARRDELAY_COL = 5;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length <= ARRDELAY_COL) return;
        String flightOrigin = tokens[ORIGIN_COL];
        String flightDest =  tokens[DEST_COL];
        String flightNum = tokens[FLIGHT_NUM_COL];
        String flightDate = tokens[DATE_COL];
        String flightDepTime = tokens[DEPTIME_COL];
        String flightArrDelay = tokens[ARRDELAY_COL];
        if (flightOrigin.length() == 0 || flightDest.length() == 0 || flightNum.length() == 0 ||
                flightDepTime.length() == 0 || flightArrDelay.length() == 0 || flightDate.length() == 0) return;
        int flightDepTime_rel =(int)Double.parseDouble(flightDepTime);
        float flightArrDelay_rel = Float.parseFloat(flightArrDelay);

        Calendar calendar = new GregorianCalendar();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        FlightRecord flightRecord = new FlightRecord(flightOrigin, flightDest, flightNum, flightDate,
                flightDepTime_rel, flightArrDelay_rel);

        try {
            Date theDate = dateFormat.parse(flightDate);
            calendar.setTime(theDate);

            if (flightDepTime_rel <= 1200) {
               calendar.add(Calendar.DATE, 2);
               String nextFlightDate = dateFormat.format(calendar.getTime());
               String tmpKey = flightDate + "," + nextFlightDate + ","  + flightDest;
               context.write(new Text(tmpKey), flightRecord);
            } else {
                calendar.add(Calendar.DATE, -2);
                String firstFlightDate = dateFormat.format(calendar.getTime());
                String tmpKey = firstFlightDate + "," + flightDate + "," + flightOrigin;
                context.write(new Text(tmpKey), flightRecord);
            }
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
