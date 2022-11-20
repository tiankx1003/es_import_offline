package io.github.tiankx1003;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:07
 */

@SpringBootApplication
@Configuration
@ComponentScan("io.github.tiankx1003")
@EnableAsync
public class ApplicationEntry {
    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(ApplicationEntry.class, args);
        ctx.registerShutdownHook();
    }
}
