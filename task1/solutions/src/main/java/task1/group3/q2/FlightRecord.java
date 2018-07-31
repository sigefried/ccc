package task1.group3.q2;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class FlightRecord implements WritableComparable<FlightRecord> {
    // Some data
    private Text source = new Text();
    private Text destination = new Text();
    private Text flightNumber = new Text();
    private Text flightDate = new Text();
    private IntWritable flightDepTime = new IntWritable();
    private FloatWritable flightArrDelay = new FloatWritable();

    public FlightRecord() {}

    public FlightRecord(String source, String destination, String flightNumber,
                        String flightDate, int flightDepTime, float flightArrDelay) {

        this.source.set(source);
        this.destination.set(destination);
        this.flightNumber.set(flightNumber);
        this.flightDate.set(flightDate);
        this.flightDepTime.set(flightDepTime);
        this.flightArrDelay.set(flightArrDelay);
    }

    public FlightRecord(FlightRecord other) {

        this.source.set(other.getSource().toString());
        this.destination.set(other.getDestination().toString());
        this.flightNumber.set(other.getFlightNumber().toString());
        this.flightDate.set(other.getFlightDate().toString());
        this.flightDepTime.set(other.getFlightDepTime().get());
        this.flightArrDelay.set(other.getFlightArrDelay().get());
    }


    public int compareTo(FlightRecord flightRecord) {
        int rst = this.source.compareTo(flightRecord.getSource());

        if (rst == 0) {
            rst = source.compareTo(flightRecord.getSource());
        }

        return rst;
    }

    public void write(DataOutput dataOutput) throws IOException {
        source.write(dataOutput);
        destination.write(dataOutput);
        flightNumber.write(dataOutput);
        flightDate.write(dataOutput);
        flightDepTime.write(dataOutput);
        flightArrDelay.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        source.readFields(dataInput);
        destination.readFields(dataInput);
        flightNumber.readFields(dataInput);
        flightDate.readFields(dataInput);
        flightDepTime.readFields(dataInput);
        flightArrDelay.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        int result = source != null ? source.hashCode() : 0;
        result = 11 * result + (destination != null ? destination.hashCode() : 0);
        result = 7 * result + (flightNumber != null ? flightNumber.hashCode() : 0);
        result = 13 * result + (flightDate != null ? flightDate.hashCode() : 0);
        result = 17 * result + (flightArrDelay != null ? flightArrDelay.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s", source, destination,
                flightNumber, flightDate, flightDepTime, flightArrDelay);
    }

    public void setSource(Text source) {
        this.source = source;
    }

    public void setDestination(Text destination) {
        this.destination = destination;
    }

    public void setFlightNumber(Text flightNumber) {
        this.flightNumber = flightNumber;
    }

    public void setFlightDate(Text flightDate) {
        this.flightDate = flightDate;
    }

    public void setFlightDepTime(IntWritable flightDepTime) {
        this.flightDepTime = flightDepTime;
    }

    public void setFlightArrDelay(FloatWritable flightArrDelay) {
        this.flightArrDelay = flightArrDelay;
    }

    public Text getSource() {
        return source;
    }

    public Text getDestination() {
        return destination;
    }

    public Text getFlightNumber() {
        return flightNumber;
    }

    public Text getFlightDate() {
        return flightDate;
    }

    public IntWritable getFlightDepTime() {
        return flightDepTime;
    }

    public FloatWritable getFlightArrDelay() {
        return flightArrDelay;
    }
}