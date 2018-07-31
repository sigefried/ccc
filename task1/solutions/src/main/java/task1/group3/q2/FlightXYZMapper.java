package task1.group3.q2;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
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


public class FlightXYZMapper extends Mapper<LongWritable, Text, Text, FlightRecord> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) return;
        String[] tokens = value.toString().split(",");
        if (tokens.length <= GlobalConfig.ARRDELAY_COL) return;
        String flightOrigin = tokens[GlobalConfig.ORIGIN_COL];
        String flightDest =  tokens[GlobalConfig.DEST_COL];
        String flightNum = tokens[GlobalConfig.FLIGHTNUM_COL];
        String flightCarrier = tokens[GlobalConfig.UNIQUECARRIER_COL];
        flightNum = flightCarrier+flightNum; // set the carrier as prefix
        String flightDepTime = tokens[GlobalConfig.DEPTIME_COL];
        String flightArrDelay = tokens[GlobalConfig.ARRDELAY_COL];
        String flightDate = tokens[GlobalConfig.FLIGHT_DATE_COL];
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
               String tmpKey = "F," + flightDate + "," + nextFlightDate + "," + flightOrigin +"," + flightDest;
               context.write(new Text(tmpKey), flightRecord);
            } else {
                calendar.add(Calendar.DATE, -2);
                String firstFlightDate = dateFormat.format(calendar.getTime());
                String tmpKey = "S," + firstFlightDate + "," + flightDate + "," + flightOrigin +"," + flightDest;
                context.write(new Text(tmpKey), flightRecord);
            }
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
