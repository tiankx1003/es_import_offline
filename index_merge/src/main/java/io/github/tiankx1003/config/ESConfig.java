package io.github.tiankx1003.config;

import lombok.Data;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:13
 */
@Data
@Component
@ConfigurationProperties(prefix = "elasticsearch")
@Configuration
public class ESConfig {

    private String clusterNodes;
    private String clusterName;

    @Bean
    public TransportClient transportClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        TransportClient client = TransportClient.settings(settings).build();
        // new TransportClient(settings, )
        for (String addr : clusterNodes.split(",")) {
            String[] addrs = addr.split(":");
            client.addTransportAddress(
                    new TransportAddress(InetAddress.getByName(addrs[0]),
                            Integer.parseInt(addrs[1])));
        }
        return client;
    }
}