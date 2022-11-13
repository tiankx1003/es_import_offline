package io.github.tiankx1003;

import io.github.tiankx1003.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:24
 */
@Component
@Slf4j
public class LeaderSelectorController extends LeaderSelectorListenerAdapter {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Autowired
    private final RegistryCenter registryCenter;

    @Autowired
    @Lazy
    private final NodeService nodeService;

    private final String LEADER_PATH = "leader";

    private LeaderSelector leaderSelector;

    public LeaderSelectorController(RegistryCenter registryCenter, NodeService nodeService) {
        this.registryCenter = registryCenter;
        this.nodeService = nodeService;
    }

    @PostConstruct
    public void init() {
        leaderSelector = new LeaderSelector(
                registryCenter.getClient(),
                registryCenter.getFullPath(LEADER_PATH), this);
        leaderSelector.autoRequeue();
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        String nodeId = nodeService.getLocalNode().getNodeId();
        log.info("Take leader ship and set leader node id to {}", nodeId);
        registryCenter.persist(LEADER_PATH, nodeId);
        latch.await();
        log.info("Node {} is not leader now", nodeId);
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        Utils.sendEmail(
                "tiankx1003@gmail.com",
                "ES_OFFLINE_SERVER ZK state changed",
                "ES_OFFLINE_SERVER ZK state changed");
        if (client.getConnectionStateErrorPolicy().isErrorState(newState)) {
            this.close();
        } else if (newState == ConnectionState.RECONNECTED) {
            log.info("Reconnect to zookeeper cluster, restart leader election and register node");
            this.start();
            nodeService.registerNode();
        }
        super.stateChanged(client, newState);
    }

    public boolean hasLeadership() {
        return leaderSelector.hasLeadership();
    }

    public void start() {
        log.info("Start leader election");
        leaderSelector.start();
    }

    public void close() {
        Utils.sendEmail(
                "tiankx1003@gmail.com",
                "ES_OFFLINE_SERVER ZK state changed",
                "Stop leader election");
        log.info("Stop leader election");
        if (hasLeadership()) {
            log.info("Clean leader id : {}", nodeService.getLocalNode().getNodeId());
            registryCenter.update(LEADER_PATH, "");
        }
        leaderSelector.close();
    }

    public String getServerLeaderNode() throws Exception {
        Participant leader = leaderSelector.getLeader();
        String id1 = leaderSelector.getId();
        String id = leader.getId();
        return id + "@" + id1;
    }
}