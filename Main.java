package estest;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import com.unboundid.util.json.JSONObject;

public class Main {
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		System.out.println( "in main" );
		EsClient esclient = new EsClient();
		//Client client = esclient.getClient();

		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs.html
		//GetResponse response = client.prepareGet("tutorial", "helloworld", "1").get();
		//Map<String, Object> result = response.getSource();
		//System.out.println(result);
		//System.out.println(result.get("message"));
		
		//========Create tweet======
		Map<String, Object> tweet1 = new HashMap<String, Object>();
		tweet1.put("user", "saju");
		tweet1.put("postDate", new Date());
		tweet1.put("message", "Hello1");
		IndexResponse inResponse = esclient.index("twitter", "tweet", "1", tweet1);

		//=========Get tweet=========
		System.out.println(esclient.get("twitter", "tweet", "1"));	

		//========Update tweet=========
		Map<String, Object> uptweet1 = new HashMap<String, Object>();
		uptweet1.put("message", "Hello2");
		UpdateResponse upResponse1 = esclient.update("twitter", "tweet", "1", uptweet1);		

		//==========Get tweet=========
		System.out.println(esclient.get("twitter", "tweet", "1"));		
		
		//=======update tweet===========
		XContentBuilder uptweet2 = jsonBuilder()
				.startObject()
					.field("message", "Hello3")
				.endObject();
		UpdateResponse upResponse2 = esclient.update("twitter", "tweet", "1", uptweet2);

		//=========Get tweet==========
		System.out.println(esclient.get("twitter", "tweet", "1"));		

		//=========upsert tweet===========
		Map<String, Object> tweet2 = new HashMap<String, Object>();
		tweet2.put("user", "John");
		tweet2.put("postDate", new Date());
		tweet2.put("message", "Hello1");
		IndexRequest indexRequest1 = EsClient.CreateIndexRequest("twitter", "tweet", "2", tweet2);		
		
		Map<String, Object> uptweet3 = new HashMap<String, Object>();
		uptweet3.put("message", "Hello2");
		UpdateRequest updateRequest1 = EsClient.CreateUpdateRequest("twitter", "tweet", "2", uptweet3);		
		
		UpdateResponse upResponse3 = esclient.upsert(updateRequest1, indexRequest1);
		
		//=========Get tweet==========
		System.out.println(esclient.get("twitter", "tweet", "2"));				
				
				
		//=========Delete tweet========
		//System.out.println(esclient.delete("twitter", "tweet", "1"));
		//System.out.println(esclient.delete("twitter", "tweet", "2"));

		//===========Get tweet=======
		//System.out.println(esclient.get("twitter", "tweet", "1"));
		
		
	}

}

