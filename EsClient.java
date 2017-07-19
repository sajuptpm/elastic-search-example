package estest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class EsClient {

	private String ClusterName = "jiocluster";
	private Client client;
	
	public EsClient() throws UnknownHostException {
		// TODO Auto-generated constructor stub
		client = getClient();
	}
	
	public Client getClient() throws UnknownHostException {
		if (client == null) {
			client = createClient();
		}
		return client;
	}
	
	private Client createClient() throws UnknownHostException {
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.4/transport-client.html
		System.out.println("Creating Client");
		Settings settings = Settings.builder()
				.put("cluster.name", ClusterName).build();
		TransportClient tclient = new PreBuiltTransportClient(settings); //need transport-5.2.0.jar
		tclient = tclient.addTransportAddress(
				new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		client = tclient;
		System.out.println("Created client");
		return client;
	}
	
	public Map<String, Object> get(String index, String type, String id) {
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs.html
		//GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
		GetResponse response = client.prepareGet(index, type, id).get();
		Map<String, Object> result = response.getSource();
		//System.out.println(result);
		//System.out.println(result.get("message"));		
		return result;
	}
	
	public IndexResponse index(String index, String type, String id, Map<String, Object> data){
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs.html
		IndexResponse response = client.prepareIndex(index, type, id).setSource(data).get();
		System.out.println("Created /" +index+"/"+type+"/" + id);
		return response;
	}
	
	public DeleteResponse delete(String index, String type, String id){
		DeleteResponse response = client.prepareDelete(index, type, id).get();
		return response;
	}
	
	public UpdateResponse update(String index, String type, String id, Map<String, Object> data){
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs.html
		UpdateResponse response = client.prepareUpdate(index, type, id).setDoc(data).get();
		System.out.println("Updated /" +index+"/"+type+"/" + id);
		return response;
	}
	
	public UpdateResponse update(String index, String type, String id, XContentBuilder data){
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs.html
		UpdateResponse response = client.prepareUpdate(index, type, id).setDoc(data).get();
		System.out.println("Updated /" +index+"/"+type+"/" + id);
		return response;
	}

	public UpdateResponse update(UpdateRequest updateRequest) throws InterruptedException, ExecutionException{
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs.html
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
		//https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs.html
		UpdateRequest _updateRequest = updateRequest.upsert(indexRequest);
		return client.update(_updateRequest).get();
	}
	
}



