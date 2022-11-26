package io.github.tiankx1003;

import com.google.common.base.Charsets;
import io.github.tiankx1003.utils.ZKException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:22
 */
@Component
@Slf4j
public class RegistryCenter {

    @Value("${curator.rootPath:/es_import_offline}")
    private String rootPath;

    @Autowired
    @Getter
    private CuratorFramework client;

    public boolean isExisted(String path) {
        String fullPath = getFullPath(path);
        try {
            return null != client.checkExists().forPath(fullPath);
        } catch (Exception e) {
            handleZKException(e);
        }
        return false;
    }


    public void persist(final String path, final String value) {
        String fullPath = this.getFullPath(path);
        log.info("Persist zk path [{}] value [{}]", fullPath, value);
        try {
            if (!isExisted(path)) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(fullPath, value.getBytes(Charsets.UTF_8));
            } else {
                update(path, value);
            }
        } catch (Exception e) {
            handleZKException(e);
        }
    }

    public void update(final String path, final String value) {
        String fullPath = getFullPath(path);
        log.info("Update zk path [{}] value [{}]", fullPath, value);
        try {
            client.transaction().forOperations(client.transactionOp().check().forPath(fullPath), client.transactionOp().setData().forPath(fullPath, value.getBytes(Charsets.UTF_8)));

        } catch (Exception e) {
            handleZKException(e);
        }
    }

    public void persistEphemeral(final String path, final String value) {
        String fullPath = getFullPath(path);
        log.info("Persist ephemeral zk path [{}] value [{}]", fullPath, value);
        try {
            if (isExisted(path)) {
                client.delete().deletingChildrenIfNeeded().forPath(fullPath);
            }
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(fullPath, value.getBytes(Charsets.UTF_8));
        } catch (final Exception e) {
            handleZKException(e);
        }
    }

    public List<String> getChildrenPaths(final String path) {
        String fullPath = getFullPath(path);
        try {
            List<String> children = client.getChildren().forPath(fullPath);
            Collections.sort(children);
            return children;
        } catch (final Exception e) {
            handleZKException(e);
            return Collections.emptyList();
        }
    }

    public int getNumChildren(final String path) {
        String fullPath = getFullPath(path);
        try {
            Stat stat = client.checkExists().forPath(fullPath);
            if (null != stat) {
                return stat.getNumChildren();
            }
        } catch (final Exception e) {
            handleZKException(e);
        }
        return 0;
    }

    public String getValue(final String path) {
        String fullPath = getFullPath(path);
        try {
            return new String(client.getData().forPath(fullPath), Charsets.UTF_8);
        } catch (final Exception e) {
            handleZKException(e);
            return null;
        }
    }

    public void delete(final String path) {
        String fullPath = getFullPath(path);
        log.info("Delete path [{}]", fullPath);
        try {
            client.delete().deletingChildrenIfNeeded().forPath(fullPath);
        } catch (final Exception e) {
            handleZKException(e);
        }
    }

    private void handleZKException(Exception e) {
        ZKException zkException = new ZKException(e);
        log.error("Got ZKException", e);
        throw zkException;
    }

    public String getFullPath(String path) {
        return ZKPaths.makePath(rootPath, path);
    }

    public String getShortPath(String fullPath) {
        return fullPath.replace(rootPath, "");
    }


    public void close() {
        log.info("Registry center close");
        client.close();
    }

    public InterProcessMutex getLock(String path) {
        String fullPath = getFullPath(path);
        return new InterProcessMutex(client, fullPath);
    }
}