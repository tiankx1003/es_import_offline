package io.github.tiankx1003;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.tiankx1003.utils.IpUtils;
import io.github.tiankx1003.utils.Utils;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:23
 */
@Component
@Slf4j
public class NodeService {

    private static final String NODE_PATH = "nodes";
    private static final String ES_NODE_JOINER = ",";
    private static final String ES_UI_COLLECT_INDEX_ALIISE = "custom";
    private Thread buildThread;
    private boolean isSparkComplte = false;


    @Autowired
    private RegistryCenter registryCenter;

    @Getter
    private Node localNode;

    @Autowired
    private LeaderSelectorController leaderSelectorController;

    @Autowired
    private IndicesChangeController indicesChangeController;

    @Autowired
    private ESClient esClient;

    @Autowired
    private HdfsClient hdfsClient;

    @Autowired
    private IndexBuilder indexBuilder;

    private volatile boolean started = false;
    private static final String ASSIGN_FLAG = "_assigned";
    private String ZK_INDEX_PATH_PREFIX = "/es_import_offline/indices";

    public NodeService() {
    }

    @PostConstruct
    public void init() throws Exception {
        String nodeId = IpUtils.getId();
        localNode = new Node(nodeId);
        this.start();
    }

    public void start() throws Exception {
        if (started) {
            return;
        }
        log.info("Start node {}", localNode.getNodeId());
        this.registerNode();
        leaderSelectorController.start();
        indicesChangeController.start();
    }

    @PreDestroy
    public void close() throws IOException {
        log.info("Close node {}", this.localNode);
        leaderSelectorController.close();
        indicesChangeController.close();
        registryCenter.close();
    }

    public void registerNode() {
        log.info("Register node {} on path {}", localNode.getNodeId(),
                registryCenter.getFullPath(localNode.getZKPath()));
        registryCenter.persistEphemeral(localNode.getZKPath(), "");
        this.updateESNodeInfo();
    }

    public class BuildIndexWorker extends Thread {
        private String indexPath;
        private String data;

        public BuildIndexWorker(String indexPath, String data) {
            this.indexPath = indexPath;
            this.data = data;
        }

        @Override
        public void run() {
            String indexNodePath = indexPath + "/" + localNode.getNodeId();
            // 在开始build之前先各自更新一下当前节点上运行es data node数量，防止data node掉线
            updateESNodeInfo();
            JSONObject configData = JSON.parseObject(data);

            try {
                String serverLeaderNode = leaderSelectorController.getServerLeaderNode();
                log.info(serverLeaderNode);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (leaderSelectorController.hasLeadership()) {
                assignShards(configData, indexPath);
                registryCenter.persist(indexPath + "/" + ASSIGN_FLAG, "");
            }
            Map<String, List<String>> currentNodeShards = getCurrentNodeShards(indexPath, indexNodePath);
            log.info(indexNodePath + "'s id2Shards: " + currentNodeShards);
            if (MapUtils.isNotEmpty(currentNodeShards)) {
                boolean success = indexBuilder.build(currentNodeShards, configData);
                if (success) {
                    registryCenter.delete(indexNodePath);
                    log.info("Build index for {} complete", indexNodePath);
                }
            }

            if (leaderSelectorController.hasLeadership()) {
                waitAllNodeComplete(indexPath);
                registryCenter.delete(indexPath);
                String indexName = configData.getString("indexName");
                log.info("Trigger cluster state change for index {}", indexName);
                esClient.triggerClusterChange(indexName);
                List<String> alies2OldIndex;
                byte esClusterState;
                while (true) {
                    try {
                        log.info("cluster state isn't green, wait 20s to change alis to new index and then set replica number to 2");
                        Thread.sleep(20000);

                        esClusterState = esClient.getESClusterState(indexName);
                        if (esClusterState == 0) {
                            log.info("cluster state is green,wait 1 min then change alis to new index ");
                            Thread.sleep(60000);
                            break;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
                // don't do this to aovid cluster clash
//                esClient.updateReplicNum(indexName,2);
                alies2OldIndex = esClient.updateAlies(indexName, ES_UI_COLLECT_INDEX_ALIISE);
                log.info("close index:" + alies2OldIndex);
                // colse old index
                if (alies2OldIndex.size() != 0) {
                    for (String oldIndex : alies2OldIndex) {
                        esClient.closeIndex(oldIndex);
                    }
                }
                // delete old index
                String deleteIndexName = Utils.getBeforeIndexName(indexName, -2, "_");
                log.info("delete old index:" + deleteIndexName);
                esClient.deleteIndex(deleteIndexName);
                log.info("set index disable_allocation is true " + indexName);
                esClient.disableShardRelocation(indexName);

                log.info("Build index for {} all complete", indexPath);

                Utils.sendEmail("tiankx1003@gmail.com", "Task_8128", "task 8128 succeeded which custom index is:" + indexName);
            }
        }
    }

    public void buildIndex(String indexPath, String data) {
        buildThread = new BuildIndexWorker(indexPath, data);
        buildThread.start();
    }

    public void markIndexComplete(String data) {
        isSparkComplte = true;
        JSONObject configData = JSON.parseObject(data);
        boolean completed = configData.getString("state").equalsIgnoreCase("completed");
        if (completed) {
            String indexName = configData.getString("indexName");
            indexBuilder.markIndexCompleted(indexName);
        }
    }

    private void waitAllNodeComplete(String indexPath) {
        int leftCount;
        while ((leftCount = registryCenter.getNumChildren(indexPath)) != 1) {
            try {
                Thread.sleep(1000);
                log.info("Wait all node complete, [{}] node left ,sleep 1000 ms", leftCount - 1);
            } catch (InterruptedException e) {
                log.error("Wait all node complete error", e);
            }
        }
        log.info("All node completed for indexPath : {}", indexPath);
    }

    public Map<String, String[]> getAllRegisteredNode() {
        Map<String, String[]> result = Maps.newHashMap();
        List<String> childrenPaths = registryCenter.getChildrenPaths(NODE_PATH);

        Utils.sendEmail("tiankx1003@gmail.com", "All registered node", "alive node num: " + childrenPaths.size() + "  and detail: " + childrenPaths);

        log.info("All registered node is {}", childrenPaths);
        for (String path : childrenPaths) {
            String esNodesStr = registryCenter.getValue(NODE_PATH + "/" + path);
            String[] esNodes = esNodesStr.split(ES_NODE_JOINER);
            log.info("Node to ES node is {} : {}", path, Lists.newArrayList(esNodes));
            if (ArrayUtils.isNotEmpty(esNodes) && StringUtils.isNotEmpty(esNodesStr)) {
                result.put(path, esNodes);
            }
        }
        return result;
    }

    private void assignShards(JSONObject configData, String indexPath) {
        log.info("Start assign shard");
        String indexName = configData.getString("indexName");
        int numberShards = configData.getInteger("numberShards");
        esClient.createIndexFirst(indexName, 0, numberShards);
        log.info("create index finished: " + indexName + "---" + "--" + numberShards);
        String mappingString = null;
        try {
            mappingString = hdfsClient.readMappingJson(Paths
                    .get(configData.getString("hdfsWorkDir"))
                    .resolve(configData.getString("indexName"))
                    .resolve("mapping.json")
                    .toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        esClient.putMapping(JSON.parseObject(mappingString), indexName, configData.getString("typeName"));

        Map<String, String[]> allNodes = this.getAllRegisteredNode();

        List<String> serverAliveIds = allNodes.values().stream()
                .flatMap(x -> Lists.newArrayList(x).stream())
                .filter(x -> StringUtils.isNotEmpty(x))
                .collect(Collectors.toList());

        log.info("Server alive node id sequence is : {}", serverAliveIds);

        List<String> ids = new ArrayList<>();
        Map<Integer, String> relocationShards = new HashMap<>();

        // only solve server node down but es node is alive
        // when es node down but server node alive is ok
        Map<Integer, String> nodesShards = esClient.getNodesShards(indexName);
        log.info("es cluster first asssion shard distribution: " + nodesShards);
        for (Map.Entry<Integer, String> node2ShardsInEsCluster : nodesShards.entrySet()) {
            Integer sharId = node2ShardsInEsCluster.getKey();
            String clusterNodeId = node2ShardsInEsCluster.getValue();
            if (serverAliveIds.contains(clusterNodeId)) {
                ids.add(sharId, clusterNodeId);
            } else {
                relocationShards.put(sharId, clusterNodeId);
            }
        }
        log.info("can't assign shard list: " + relocationShards);
        for (Map.Entry<Integer, String> relocationShard : relocationShards.entrySet()) {
            Integer shardId = relocationShard.getKey();
            String oldNodeId = relocationShard.getValue();
            for (String newNodeId : ids) {
                if (ids.indexOf(newNodeId) == ids.lastIndexOf(newNodeId)) {
                    boolean moveResult = esClient.relocationShards(indexName, shardId, oldNodeId, newNodeId);
                    if (moveResult) {
                        log.info("move shard [" + shardId + "] from [" + oldNodeId + "] to [" + newNodeId + "] success");
                        ids.add(shardId, newNodeId);
                        break;
                    } else {
                        log.info("move shard [" + shardId + "] from [" + oldNodeId + "] to [" + newNodeId + "] fail, move it to next node");
                    }
                }
            }
        }
        log.info("ES cluster final node and shard route is : {}", ids);
        log.info("Node to ES node is : {}", allNodes);
        allNodes.entrySet().forEach(x -> {
            Map<Integer, String> newNodesShards = esClient.getNodesShards(indexName);
            String nodeId = x.getKey();
            Map<String, List<Integer>> idToShards = Maps.newHashMap();
            List<String> clustNodeIdList = Arrays.asList(x.getValue());
            for (Map.Entry<Integer, String> shardNode : newNodesShards.entrySet()) {
                String clusternodeId = shardNode.getValue();
                if (clustNodeIdList.contains(clusternodeId)) {
                    List<Integer> shards = idToShards.get(clusternodeId);
                    if (shards == null) {
                        shards = new ArrayList<>();
                        idToShards.put(clusternodeId, shards);
                    }
                    shards.add(shardNode.getKey());
                }
            }
            if (MapUtils.isNotEmpty(idToShards)) {
                String idToShardsJSON = JSON.toJSONString(idToShards);
                registryCenter.persist(indexPath + "/" + nodeId, idToShardsJSON);
            }
        });
        Utils.sendEmail("tiankx1003@gmail.com", "master finish assign shard", "master finish assign shard");
        log.info("End assign shard");
    }

    private Map<String, List<String>> getCurrentNodeShards(String indexPath, String indexNodePath) {
        Map<String, List<String>> result = Maps.newHashMap();
        while (true) {
            if (registryCenter.isExisted(indexPath + "/" + ASSIGN_FLAG)) {
                break;
            }
            try {
                Thread.sleep(1000);
                log.info("Wait shard assign and sleep 1000 ms");
            } catch (InterruptedException e) {
                log.error("Wait shard assign error", e);
            }
        }
        if (registryCenter.isExisted(indexNodePath)) {
            JSONObject data = JSON.parseObject(registryCenter.getValue(indexNodePath));
            data.keySet().forEach(x -> result.put(x, data.getJSONArray(x).toJavaList(String.class)));
        }
        return result;
    }

    public void updateESNodeInfo() {
        Set<String> nodeNames = esClient.getNodeNameOnHost();
        String nodes = String.join(ES_NODE_JOINER, nodeNames);
        log.info("Update local es node info {}", nodes);
        registryCenter.update(localNode.getZKPath(), nodes);
    }

    @Data
    public static class Node {

        public Node(String nodeId) {
            this.nodeId = nodeId;
        }


        private String nodeId;

        public String getZKPath() {
            return NODE_PATH + "/" + nodeId;
        }

    }
}