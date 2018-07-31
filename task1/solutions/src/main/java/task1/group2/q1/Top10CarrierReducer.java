package task1.group2.q1;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import task1.DynamoDbUtilities;
import task1.group2.TextArrayWritable;

import java.io.IOException;
import java.util.*;


public class Top10CarrierReducer extends Reducer<Text, TextArrayWritable, Text, Text> {

    private AmazonDynamoDB dynamoDB;
    private String tableName = "q21";


    @Override
    protected void setup(Context context) throws AmazonClientException {
        dynamoDB = DynamoDbUtilities.createAmazonDynamoDBInstance();
    }


    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        String origin = key.toString();
        PriorityQueue<Entry> pq = new PriorityQueue<Entry>();
        for (TextArrayWritable it : values) {
            Text[] texts = (Text[]) it.toArray();
//            System.out.println(Arrays.toString(texts));
            String carrier = texts[0].toString();
            Double delay = Double.valueOf(texts[1].toString());
            Entry tmp = new Entry(delay, carrier);
            pq.offer(tmp);
            if (pq.size() > 10) pq.poll();
        }
        store(context, origin, pq);
    }


    private void store(Context context, String origin, PriorityQueue<Entry> pq) throws IOException, InterruptedException {
        Map<String,List<WriteRequest>> itemMap = new HashMap<String, List<WriteRequest>>();
        List<WriteRequest> writeRequestList =  new ArrayList<WriteRequest>();

        for (Entry it : pq) {
//            Map<String, AttributeValue> item = newItem(origin, it.getPerformance(), it.getDest());
//            PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
            PutRequest putReq = newPutRequest(origin, it.getPerformance(), it.getCarrier());
            WriteRequest writeRequest = new WriteRequest(putReq);
            writeRequestList.add(writeRequest);
//
//            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
//            System.err.println("Result: " + putItemResult);
        }
        itemMap.put(tableName, writeRequestList);
        BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
        batchWriteItemRequest.withRequestItems(itemMap);
        BatchWriteItemResult result = dynamoDB.batchWriteItem(batchWriteItemRequest);
        System.err.printf("Airport : %s Result: %s\n", origin, result);
    }

    private static PutRequest newPutRequest(String airport, Double depdelay, String carrier) {
        PutRequest putRequest = new PutRequest();
        putRequest.addItemEntry("airport", new AttributeValue(airport));
        putRequest.addItemEntry("depdelay", new AttributeValue().withN(Double.toString(depdelay)));
        putRequest.addItemEntry("carrier", new AttributeValue(carrier));

        return putRequest;
    }

    private static Map<String, AttributeValue> newItem(String airport, Double depdelay, String carrier) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("airport", new AttributeValue(airport));
        item.put("depdelay", new AttributeValue().withN(Double.toString(depdelay)));
        item.put("carrier", new AttributeValue(carrier));

        return item;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }


    private class Entry implements Comparable<Entry> {
        private String carrier;
        private Double performance;

        public Entry(Double performance, String carrier) {
            this.performance = performance;
            this.carrier = carrier;
        }

        public int compareTo(Entry o) {
            if (o.performance.compareTo(performance) == 0) {
                return o.carrier.compareTo(carrier);
            }
            return o.performance.compareTo(performance);
        }

        public String toString() {
            return "{ " + carrier + " : " + performance + "}";
        }

        public String getCarrier() {
            return carrier;
        }

        public Double getPerformance() {
            return performance;
        }

        public void setCarrier(String carrier) {
            this.carrier = carrier;
        }

        public void setPerformance(Double performance) {
            this.performance = performance;
        }
    }
}
