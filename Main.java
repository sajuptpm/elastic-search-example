/*
 * 
 * https://github.com/jprante/elasticsearch-client
 * https://github.com/elastic/elasticsearch
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.2/index.html
 * 
 */

package estest;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Main {
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		System.out.println( "in main" );

		DocumentAPIs.testDocumentAPIs();
		
	}

}

