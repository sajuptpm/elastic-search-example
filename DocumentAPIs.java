/*
 * 
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.2/java-docs.html
 * 
 * 
 * This section describes the following CRUD APIs:
 * 
 * 
 * Single document APIs
 * 		Index API
 * 		Get API
 * 		Delete API
 * 		Delete By Query API
 * 		Update API
 * 
 * Multi-document APIs
 * 		Multi Get API
 * 		Bulk API
 * 
 *  
*/

package estest;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

public class DocumentAPIs extends EsClient{

	public DocumentAPIs() throws UnknownHostException {
		super();
	}

	public static void testDocumentAPIs() throws IOException, InterruptedException, ExecutionException {
		
		System.out.println( "in testDocumentAPIs" );
		
		DocumentAPIs esclient = new DocumentAPIs();
		//Client client = esclient.getClient();

		//GetResponse response = client.prepareGet("tutorial", "helloworld", "1").get();
		//Map<String, Object> result = response.getSource();
		//System.out.println(result);
		//System.out.println(result.get("message"));
		
		System.out.println("========Create tweet======");	
		Map<String, Object> tweet1 = new HashMap<String, Object>();
		tweet1.put("user", "saju");
		tweet1.put("postDate", new Date());
		tweet1.put("message", "Hello1");
		IndexResponse inResponse = esclient.index("twitter", "tweet", "1", tweet1);

		System.out.println("=========Get tweet=========");
		System.out.println(esclient.get("twitter", "tweet", "1"));	

		System.out.println("========Update tweet=========");
		Map<String, Object> uptweet1 = new HashMap<String, Object>();
		uptweet1.put("message", "Hello2");
		UpdateResponse upResponse1 = esclient.update("twitter", "tweet", "1", uptweet1);		

		System.out.println("==========Get tweet=========");
		System.out.println(esclient.get("twitter", "tweet", "1"));		
		
		System.out.println("=======update tweet===========");
		XContentBuilder uptweet2 = jsonBuilder()
				.startObject()
					.field("message", "Hello3")
				.endObject();
		UpdateResponse upResponse2 = esclient.update("twitter", "tweet", "1", uptweet2);

		System.out.println("=========Get tweet==========");
		System.out.println(esclient.get("twitter", "tweet", "1"));		

		System.out.println("=========upsert tweet===========");
		Map<String, Object> tweet2 = new HashMap<String, Object>();
		tweet2.put("user", "John");
		tweet2.put("postDate", new Date());
		tweet2.put("message", "Hello1");
		IndexRequest indexRequest1 = DocumentAPIs.CreateIndexRequest("twitter", "tweet", "2", tweet2);		
		
		Map<String, Object> uptweet3 = new HashMap<String, Object>();
		uptweet3.put("message", "Hello2");
		UpdateRequest updateRequest1 = DocumentAPIs.CreateUpdateRequest("twitter", "tweet", "2", uptweet3);		
		
		UpdateResponse upResponse3 = esclient.upsert(updateRequest1, indexRequest1);
		
		System.out.println("=========Get tweet==========");
		System.out.println(esclient.get("twitter", "tweet", "2"));				
				
				
		//System.out.println("=========Delete tweet========");
		//System.out.println(esclient.delete("twitter", "tweet", "1"));
		//System.out.println(esclient.delete("twitter", "tweet", "2"));

		//System.out.println("===========Get tweet=======");
		//System.out.println(esclient.get("twitter", "tweet", "1"));

		
		System.out.println("=========Delete by Query API=============");
		MatchQueryBuilder matchquery1 = QueryBuilders.matchQuery("user", "John");
		long delcount1 = esclient.deleteByQuery("twitter", matchquery1);
		System.out.println("Deleted " + delcount1 + " tweets");
		
		//System.out.println("=========Delete by Query API async=============");
		//MatchQueryBuilder matchquery2 = QueryBuilders.matchQuery("user", "John");
		//esclient.deleteByQueryAsync("twitter", matchquery2);

	}
	
	
	public Map<String, Object> get(String index, String type, String id) {
		//GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
		GetResponse response = client.prepareGet(index, type, id).get();
		Map<String, Object> result = response.getSource();
		//System.out.println(result);
		//System.out.println(result.get("message"));		
		return result;
	}
	
	public IndexResponse index(String index, String type, String id, Map<String, Object> data){
		IndexResponse response = client.prepareIndex(index, type, id).setSource(data).get();
		System.out.println("Created /" +index+"/"+type+"/" + id);
		return response;
	}
	
	public DeleteResponse delete(String index, String type, String id){
		DeleteResponse response = client.prepareDelete(index, type, id).get();
		return response;
	}
	
	public UpdateResponse update(String index, String type, String id, Map<String, Object> data){
		UpdateResponse response = client.prepareUpdate(index, type, id).setDoc(data).get();
		System.out.println("Updated /" +index+"/"+type+"/" + id);
		return response;
	}
	
	public UpdateResponse update(String index, String type, String id, XContentBuilder data){
		UpdateResponse response = client.prepareUpdate(index, type, id).setDoc(data).get();
		System.out.println("Updated /" +index+"/"+type+"/" + id);
		return response;
	}

	public UpdateResponse update(UpdateRequest updateRequest) throws InterruptedException, ExecutionException{
		UpdateResponse response = client.update(updateRequest).get();
		return response;
	}	
	
	public static IndexRequest CreateIndexRequest(String index, String type, String id, Map<String, Object> data){
		IndexRequest indexRequest = new IndexRequest(index, type, id)
		        .source(data);
		return indexRequest;
	}
	
	public static UpdateRequest CreateUpdateRequest(String index, String type, String id, Map<String, Object> data){
		UpdateRequest updateRequest = new UpdateRequest(index, type, id)
		        .doc(data);
		return updateRequest;
	}	
	
	public UpdateResponse upsert(UpdateRequest updateRequest,IndexRequest indexRequest) throws InterruptedException, ExecutionException{
		UpdateRequest _updateRequest = updateRequest.upsert(indexRequest);
		return client.update(_updateRequest).get();
	}	

	public long deleteByQuery(String index, MatchQueryBuilder query) {
		BulkIndexByScrollResponse response =
			    DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
			        .filter(query) 
			        .source(index)                                  
			        .get();
			long deleted = response.getDeleted();
			return deleted;
	}

	public void deleteByQueryAsync(String index, MatchQueryBuilder query) {
		/*
			Todo: Fix error
		*/
		DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
	    .filter(query)                  
	    .source(index)                                                   
	    .execute(new ActionListener<BulkIndexByScrollResponse>() {           
	        @Override
	        public void onResponse(BulkIndexByScrollResponse response) {
	            long deleted = response.getDeleted();
	            System.out.println("Deleted " + deleted + " tweets");
	        }
	        @Override
	        public void onFailure(Exception e) {
	            // Handle the exception
	        }
	    });	
	}	
	

}
