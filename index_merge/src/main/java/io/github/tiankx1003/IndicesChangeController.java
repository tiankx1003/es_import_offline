package io.github.tiankx1003;

import com.google.common.base.Charsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:25
 */
@Component
@Slf4j
public class IndicesChangeController implements PathChildrenCacheListener {

    private static final String INDICES_PATH = "indices";


    @Autowired
    private final RegistryCenter registryCenter;

    @Autowired
    @Lazy
    private final NodeService nodeService;

    private PathChildrenCache cache;

    public IndicesChangeController(RegistryCenter registryCenter, NodeService nodeService) {
        this.registryCenter = registryCenter;
        this.nodeService = nodeService;
    }


    @PostConstruct
    private void init() {
        cache = new PathChildrenCache(registryCenter.getClient(),
                registryCenter.getFullPath(INDICES_PATH), true);
        cache.getListenable().addListener(this);
    }

    public void start() throws Exception {
        log.info("Start path children cache on {}", registryCenter.getFullPath(INDICES_PATH));
        this.cache.start();
    }

    public void close() throws IOException {
        log.info("Path children cache close");
        cache.close();
    }


    @Override
    public void childEvent(CuratorFramework curatorFramework,
                           PathChildrenCacheEvent event) {
        Type eventType = event.getType();
        String data = null;
        String path = null;

        if (event.getData() != null) {
            data = new String(event.getData().getData(), Charsets.UTF_8);
            path = registryCenter.getShortPath(event.getData().getPath());
        }

        log.info("Got event type [{}] path [{}] data {} ", eventType, path, data);

        // every server member do this
        switch (event.getType()) {
            case CHILD_ADDED:
                nodeService.buildIndex(path, data);
                break;
            case CHILD_UPDATED:
                nodeService.markIndexComplete(data);
                break;
            // TODO: other event type process
            case CHILD_REMOVED:
                log.info("CHILD_REMOVED");
                break;
            case CONNECTION_LOST:
                log.info("CONNECTION_LOST");
                break;
            case CONNECTION_RECONNECTED:
                log.info("CONNECTION_RECONNECTED");
                break;
            case CONNECTION_SUSPENDED:
                log.info("CONNECTION_SUSPENDED");
                break;
            default:
                break;
        }
    }

}