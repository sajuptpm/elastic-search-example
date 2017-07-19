/*
 * 
 * 
 * 
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.2/java-docs.html
 * 
 * 
 * 
 * 
 * 
 */

package estest;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class EsClient {

	private String ClusterName = "jiocluster";
	protected Client client;
	
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
	
}



