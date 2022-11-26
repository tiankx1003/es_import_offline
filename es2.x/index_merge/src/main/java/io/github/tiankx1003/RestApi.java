package io.github.tiankx1003;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:20
 */
@RestController
@RequestMapping(value = "/ws/v1/applications")
@Slf4j
public class RestApi {

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public String test() {
        log.info("test");
        return "test";
    }
}
