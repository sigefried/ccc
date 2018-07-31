package task1.group3.q2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import task1.DynamoDbUtilities;

import java.io.IOException;
import java.util.*;


public class FlightSchedulerReducer extends Reducer<Text, FlightRecord, Text, Text> {


    private AmazonDynamoDB dynamoDB;
    private String tableName = "q32";
    private Set<String> candidateDates = new HashSet<String>();

    @Override
    protected void setup(Context context) throws AmazonClientException {
        dynamoDB = DynamoDbUtilities.createAmazonDynamoDBInstance();
        candidateDates.add("2008-03-04");
        candidateDates.add("2008-09-09");
        candidateDates.add("2008-04-01");
        candidateDates.add("2008-07-12");
        candidateDates.add("2008-06-10");
        candidateDates.add("2008-01-01");
    }

    public void reduce(Text key, Iterable<FlightRecord> values, Context context) throws IOException, InterruptedException {

        List<FlightRecord> arrFirst = new ArrayList<FlightRecord>();
        List<FlightRecord> arrSecond = new ArrayList<FlightRecord>();
        FlightRecord firstHalf = null;
        FlightRecord secondHalf = null;
        for (FlightRecord flightRecord : values) {
            int time = flightRecord.getFlightDepTime().get();
            if (time <= 1200) {
                firstHalf = new FlightRecord(flightRecord);
                arrFirst.add(firstHalf);
            } else {
                secondHalf = new FlightRecord(flightRecord);
                arrSecond.add(secondHalf);
            }
        }
        store(context, arrFirst, arrSecond);
    }

    private void store(Context context, List<FlightRecord> arrFirst, List<FlightRecord> arrSecond) throws IOException, InterruptedException {
        List<WriteRequest> writeRequestList = new ArrayList<WriteRequest>();
        for (FlightRecord first : arrFirst) {
            for (FlightRecord second : arrSecond) {
                String firstFlightDate = first.getFlightDate().toString();
                if (!candidateDates.contains(firstFlightDate)) continue;
                String XYZ = first.getSource().toString() + "-" + first.getDestination().toString() + "-"
                        + second.getDestination().toString();
                double delay = first.getFlightArrDelay().get() + second.getFlightArrDelay().get();
                String firstFlight = first.getFlightNumber().toString();
                String secondFlight = second.getFlightNumber().toString();
                String secondFlightDate = second.getFlightDate().toString();
                PutRequest putReq = newPutRequest(XYZ, delay, firstFlight, secondFlight, firstFlightDate, secondFlightDate);
                WriteRequest writeRequest = new WriteRequest(putReq);
                writeRequestList.add(writeRequest);
                if (writeRequestList.size() >= 10) {
                    this.raiseBatchedWriteRequest(writeRequestList);
                    writeRequestList.clear();
                }
            }
        }
        if (writeRequestList.size() > 0) {
            this.raiseBatchedWriteRequest(writeRequestList);
        }
    }


    private void raiseBatchedWriteRequest(List<WriteRequest> writeRequestList) {
        Map<String, List<WriteRequest>> itemMap = new HashMap<String, List<WriteRequest>>();
        itemMap.put(tableName, writeRequestList);
        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
        batchWriteItemRequest.withRequestItems(itemMap);
        BatchWriteItemResult result = dynamoDB.batchWriteItem(batchWriteItemRequest);
        System.err.printf(" Result: %s\n", result);
    }


    private static PutRequest newPutRequest(String XYZ, Double delay, String firstFlight, String secondFlight, String firstFlightDate, String secondFlightDate) {
        PutRequest putRequest = new PutRequest();
        putRequest.addItemEntry("XYZ", new AttributeValue(XYZ));
        putRequest.addItemEntry("firstFlightDate", new AttributeValue(firstFlightDate));
        putRequest.addItemEntry("firstFlight", new AttributeValue(firstFlight));
        putRequest.addItemEntry("secondFlightDate", new AttributeValue(secondFlightDate));
        putRequest.addItemEntry("secondFlight", new AttributeValue(secondFlight));
        putRequest.addItemEntry("totalDelay", new AttributeValue().withN(Double.toString(delay)));

        return putRequest;
    }

}
