package task1.group2.q2;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import task1.group2.TextArrayWritable;
import task1.DynamoDbUtilities;

import java.io.IOException;
import java.util.*;


public class Top10DestReducer extends Reducer<Text, TextArrayWritable, Text, Text> {

    private AmazonDynamoDB dynamoDB;
    private String tableName = "q22";

    protected void setup(Context context) throws AmazonClientException {
        dynamoDB = DynamoDbUtilities.createAmazonDynamoDBInstance();
    }


    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        String origin = key.toString();
        PriorityQueue<Entry> pq = new PriorityQueue<Entry>();
        for (TextArrayWritable it : values) {
            Text[] texts = (Text[]) it.toArray();
            String dest = texts[0].toString();
            Double delay = Double.valueOf(texts[1].toString());
            Entry tmp = new Entry(delay, dest);
            pq.offer(tmp);
            if (pq.size() > 10) pq.poll();
        }
        store(context, origin, pq);
    }


    private void store(Context context, String origin, PriorityQueue<Entry> pq) throws IOException, InterruptedException {
        Map<String, List<WriteRequest>> itemMap = new HashMap<String, List<WriteRequest>>();
        List<WriteRequest> writeRequestList =  new ArrayList<WriteRequest>();

        Set<Double> existDelay = new HashSet<Double>();
        for (Entry it : pq) {
            Double delay = it.getPerformance();
            while (existDelay.contains(delay)) {
                delay += 0.00001; // hacks deal with duplicate key error
            }
            PutRequest putReq = newPutRequest(origin, delay, it.getDest());
            WriteRequest writeRequest = new WriteRequest(putReq);
            writeRequestList.add(writeRequest);
//            System.err.println(origin+","+delay+","+it.getDest());
            existDelay.add(delay);
        }
        itemMap.put(tableName, writeRequestList);
        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
        batchWriteItemRequest.withRequestItems(itemMap);
        BatchWriteItemResult result = dynamoDB.batchWriteItem(batchWriteItemRequest);
        System.err.printf("Airport : %s Result: %s\n", origin, result);
    }

    private static PutRequest newPutRequest(String airport, Double depdelay, String dest) {
        PutRequest putRequest = new PutRequest();
        putRequest.addItemEntry("airport", new AttributeValue(airport));
        putRequest.addItemEntry("depdelay", new AttributeValue().withN(Double.toString(depdelay)));
        putRequest.addItemEntry("dest", new AttributeValue(dest));

        return putRequest;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    private class Entry implements Comparable<Entry> {
        private String dest;
        private Double performance;

        public Entry(Double performance, String dest) {
            this.performance = performance;
            this.dest = dest;
        }

        public int compareTo(Entry o) {
            if (o.performance.compareTo(performance) == 0) {
                return o.dest.compareTo(dest);
            }
            return o.performance.compareTo(performance);
        }

        public String toString() {
            return "{ " + dest + " : " + performance + "}";
        }

        public String getDest() {
            return dest;
        }

        public Double getPerformance() {
            return performance;
        }

        public void setDest(String dest) {
            this.dest = dest;
        }

        public void setPerformance(Double performance) {
            this.performance = performance;
        }
    }
}
