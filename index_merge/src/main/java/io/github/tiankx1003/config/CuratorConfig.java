package io.github.tiankx1003.config;


import lombok.Data;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:11
 */


@Data
@Component
@ConfigurationProperties(prefix = "curator")
@Configuration
public class CuratorConfig {

    private int retryCount;
    private int elapsedTimeMs;
    private String connectString;
    private int sessionTimeoutMs;
    private int connectionTimeoutMs;

    @Bean(initMethod = "start")
    public CuratorFramework curatorFramework() {
        return CuratorFrameworkFactory.newClient(
                this.getConnectString(),
                this.getSessionTimeoutMs(),
                this.getConnectionTimeoutMs(),
                new RetryNTimes(this.getRetryCount(), this.getElapsedTimeMs()));
    }

}
