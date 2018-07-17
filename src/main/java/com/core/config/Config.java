package com.core.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.elasticsearch.common.settings.Settings;
import org.springframework.context.annotation.Bean;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.springframework.beans.factory.annotation.Value;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.context.annotation.Configuration;
import org.elasticsearch.transport.client.PreBuiltTransportClient;



@Configuration
public class Config {

	@Value("${elasticSearch.cluster.name}")
	private String esClusterName;
	
	@Value("${elasticSearch.cluster.ip}")
	private String esHostIp;

	@Value("${elasticSearch.cluster.port}")
	private String port;



	@Bean
	public TransportClient getClient() throws UnknownHostException {
		Builder builder = Settings.builder();
		builder.put("cluster.name", esClusterName);
		builder.put("client.transport.sniff", true);
		builder.put("thread_pool.search.size", 5);

		Settings settings = builder.build();
        TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(esHostIp), Integer.valueOf(port));

		TransportClient transportClient = new PreBuiltTransportClient(settings);

		//可以添加多个传输地址
		transportClient.addTransportAddresses(transportAddress);

		return transportClient;
	}



}