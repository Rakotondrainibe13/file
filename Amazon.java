import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

public class Amazon {

    // curl -X GET -u myloby "https://vpc-myloby-xcxvoshgw4yexseglqlbsrcr2u.eu-west-3.es.amazonaws.com"
    private static final String host = "vpc-myloby-xcxvoshgw4yexseglqlbsrcr2u.eu-west-3.es.amazonaws.com";
    private static Region region = Region.US_WEST_2;
    private static String login="myloby";
    private static String mdp="HQ7jKGQ5:SM)!Hu";

    public static void main(String[] args) throws IOException, InterruptedException {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(login, mdp);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        AwsCredentialsProvider awsCredsProv= StaticCredentialsProvider.create(awsCreds);
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(login,mdp));
        SdkHttpClient httpClient = ApacheHttpClient.builder()
        .credentialsProvider(credentialsProvider)
        .build();
        
        try {

            OpenSearchClient client = new OpenSearchClient(
                    new AwsSdk2Transport(
                            httpClient,
                            host,
                            region,
                            AwsSdk2TransportOptions.builder()
                            .setCredentials(awsCredsProv)
                            .build()))
                            ;


            // create the index
            String index = "sampleindex";
            
            CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder().index(index).build();
            client.indices().create(createIndexRequest);

            // index data
            Map<String, Object> document = new HashMap<>();
            document.put("firstName", "Michael");
            document.put("lastName", "Douglas");
            IndexRequest documentIndexRequest = new IndexRequest.Builder()
                    .index(index)
                    .id("2")
                    .document(document)
                    .build();
            client.index(documentIndexRequest);

            // delete the index
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest.Builder().index(index).build();
            client.indices().delete(deleteRequest);
            
        } finally {
            httpClient.close();
        }
    }
}