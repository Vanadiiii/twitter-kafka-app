package me.imatveev.kafkaconsumer.service;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchService extends RestHighLevelClient {
    private ElasticSearchService(RestClientBuilder builder) {
        super(builder);
    }

    public static ElasticSearchService of(String url, String username, String password) {
        final CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(username, password)
        );

        final RestClientBuilder builder = RestClient.builder(new HttpHost(url, 443, "https"))
                .setHttpClientConfigCallback(clientBuilder -> clientBuilder.setDefaultCredentialsProvider(provider));

        return new ElasticSearchService(builder);
    }
}
