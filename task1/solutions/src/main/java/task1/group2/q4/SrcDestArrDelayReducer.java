package task1.group2.q4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import task1.DynamoDbUtilities;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class SrcDestArrDelayReducer extends Reducer<Text, FloatWritable, Text, Text> {

    private AmazonDynamoDB dynamoDB;
    private String tableName = "q24";


    @Override
    protected void setup(Context context) throws AmazonClientException {
        dynamoDB = DynamoDbUtilities.createAmazonDynamoDBInstance();
    }

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        String src_dst = key.toString();
        double sum = 0;
        int count = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count += 1;
        }
        double avg = sum / count;
        store(context, src_dst, avg);
    }


    private void store(Context context, String src_dst, double avgdelay) throws IOException, InterruptedException {
        String[] sub_tokens = src_dst.split("_");
        String src = sub_tokens[0];
        String dest = sub_tokens[1];
        Map<String, AttributeValue> item = newItem(src, dest, avgdelay);
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);

    }

    private static Map<String, AttributeValue> newItem(String src, String dest, double avgdelay) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("src-dest", new AttributeValue(src+"-"+dest));
        item.put("arrdelay", new AttributeValue().withN(Double.toString(avgdelay)));

        return item;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
