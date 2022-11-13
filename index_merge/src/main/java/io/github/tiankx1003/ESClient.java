package io.github.tiankx1003;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.Maps;
import io.github.tiankx1003.utils.IpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.fs.FsInfo.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:28
 */
@Component
@Slf4j
public class ESClient {

    @Autowired
    private TransportClient client;

    public void disableShardRelocation(String indexName){
        Settings settings = Settings.builder()
                .put("routing.allocation.disable_allocation","true").build();
        boolean result = client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).get().isAcknowledged();
        log.info("update routing.allocation.disable_allocation true action result: "+result);
    }

    public void deleteIndex(String indexName) {
        boolean exists = client.admin().indices().prepareExists(indexName).get().isExists();
        if (exists){
            boolean acknowledged = client.admin().indices().prepareDelete(indexName).get().isAcknowledged();
            log.info("delete index: " + indexName + " opretion result: " + acknowledged);
        }else {
            log.info("index not exists, delete finished");
        }
    }


    //change1 start: date 20200323, only add method
    public boolean createIndexFirst(String indexName, int replicasNum, int shardNum) {
        log.info("ready to create index:" + indexName + " number_of_replicas:" + replicasNum + " number_of_shards:" + shardNum);
        Settings settings = Settings.builder()
                .put("number_of_replicas", replicasNum)
                .put("refresh_interval", -1)
                .put("number_of_shards", shardNum).build();
        log.info("setting build: " + settings.names());
        boolean acknowledged = client.admin().indices()
                .prepareCreate(indexName)
                .setSettings(settings).get().isAcknowledged();
        log.info("finish to create index, action result is: " + acknowledged);
        return true;
    }

    //server master do this, check if server node and es node are ready to work
    public Map<Integer, String> getNodesShards(String indexName) {
        Map<Integer, String> shardAssgin = new HashMap<>();
        ClusterSearchShardsGroup[] shardsGroups = client.admin().cluster().prepareSearchShards()
                .setIndices(indexName).get().getGroups();
        for (ClusterSearchShardsGroup shardsGroup : shardsGroups) {
            ShardRouting[] shards = shardsGroup.getShards();
            for (ShardRouting shard : shards) {
                int shardId = shard.getId();
                String nodeId = shard.currentNodeId();
                shardAssgin.put(shardId, nodeId);
            }
        }
        return shardAssgin;
    }

    //only server master car do this
    //需要先禁止自动分配节点
    public boolean relocationShards(String indexName, int shardId, String fromNode, String toNode) {
        String clusterFromNodeName = getAllNodeClusterInfo().get(fromNode).get(0);
        String clusterToNodeName = getAllNodeClusterInfo().get(toNode).get(0);

        ShardId moveShardId = new ShardId(indexName, shardId);
        MoveAllocationCommand moveAllocationCommand = new MoveAllocationCommand(
                moveShardId, clusterFromNodeName, clusterToNodeName);
        boolean done = client.admin().cluster().prepareReroute().add(moveAllocationCommand).execute().actionGet().isAcknowledged();

        return done;

    }

    public Map<String, List<String>> getAllNodeClusterInfo() {
        //clusterNodeId-clusterNodeName-Ip
        HashMap<String, List<String>> nodeAllInfo = new HashMap<>();
        List<String> nodeName2Ip = new ArrayList<>();

        NodesInfoResponse clusterInfo = client.admin().cluster().prepareNodesInfo().get();
        List<NodeInfo> nodes = clusterInfo.getNodes();
        for (NodeInfo nodeInfo : nodes) {
            DiscoveryNode node = nodeInfo.getNode();
            //machine IP
            String IP = node.getHostName();
            //cluster node name
            String clusterName = node.getName();
            //cluster Ip, No means
            String clusterNodeId = node.getId();

            nodeName2Ip.add(0, clusterName);
            nodeName2Ip.add(1, IP);
            nodeAllInfo.put(clusterNodeId, nodeName2Ip);
        }
        return nodeAllInfo;
    }

    //这时没有副本
    public String getShardDataPath(String indexName, int shardId) {
        ShardStats[] shards = client.admin().indices().prepareStats(indexName).get().getShards();
        String shardDataPath = "";
        for (ShardStats shard : shards) {
            int id = shard.getShardRouting().getId();
            if (id == shardId) {
                shardDataPath = shard.getDataPath();
                log.info("get " + shardId + " path:" + shardDataPath);
            }
        }
        return shardDataPath;
    }

    public void putMapping(JSONObject mapping, String indexName, String typeName) {
        log.info("put mapping start");
        JSONObject root = new JSONObject();
        root.put("properties", mapping);

        JSONObject disabled = new JSONObject();
        disabled.put("enabled", false);
        root.put("_all", disabled);

        client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(root).execute().actionGet();
        log.info("put mapping end");
    }

    public void updateReplicNum(String indexName, int replicNum) {
        log.info("update shard's replic num to:" + replicNum);
        Settings settings = Settings.builder()
                .put("number_of_replicas", replicNum).build();
        boolean acknowledged = client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings)
                .execute().actionGet().isAcknowledged();
        log.info("update shard replic num:" + replicNum + " result is:" + acknowledged);
    }

    //    GREEN((byte) 0), YELLOW((byte) 1), RED((byte) 2);
    public byte getESClusterState(String indexName) {
        ClusterState state = client.admin().cluster().prepareState().get().getState();
        byte value = client.admin().cluster().prepareHealth(indexName).get().getStatus().value();

        return value;

    }

    public List<String> updateAlies(String indexName, String targetAlias) {
        ImmutableOpenMap<String, List<AliasMetaData>> aliasesMap =
                client.admin().indices().prepareGetAliases().get().getAliases();
        ArrayList<String> oldIndexList = new ArrayList<>();
        for (ObjectObjectCursor<String, List<AliasMetaData>> index2Aliases : aliasesMap) {
            String index = index2Aliases.key;
            String alias = index2Aliases.value.get(0).getAlias();
            if (targetAlias.equals(alias)){
                boolean removeResult = client.admin().indices().prepareAliases().removeAlias(index, alias).get().isAcknowledged();
                log.info("remove alias: " + targetAlias + " from index: " + index + " action: " + removeResult);
                if (!index.equals(indexName)){
                    oldIndexList.add(index);
                }
            }
        }
        boolean addResult = client.admin().indices().prepareAliases().addAlias(indexName, targetAlias).get().isAcknowledged();
        log.info("add alias: " + targetAlias + " to index: " + indexName + " action: " + addResult);
        return oldIndexList;
    }

    public void closeIndex(String indexName){
        boolean acknowledged = client.admin().indices().prepareClose(indexName).get().isAcknowledged();
        log.info("close index:"+indexName+" result:"+acknowledged);
    }

    //**************  change1 end **************************************

    public Set<String> getNodeNameOnHost() {
        return this.getDataNodeInfoOnHost().keySet();
    }

    public Map<String, NodeInfo> getDataNodeInfoOnHost() {
        Map<String, NodeInfo> result = Maps.newHashMap();
        NodesInfoResponse response = client.admin().cluster().prepareNodesInfo().get();
        for (NodeInfo info : response.getNodes()) {
            boolean currentHostDataNode = isCurrentHostDataNode(info);
            if (currentHostDataNode) {

                result.put(info.getNode().getId(), info);
            }
        }
        return result;
    }

    private boolean isCurrentHostDataNode(NodeInfo info) {
        try {
            return info.getNode().isDataNode() && (
                    info.getHostname().equalsIgnoreCase(IpUtils.getHostName())
                            || info.getNode().getHostAddress().equalsIgnoreCase(IpUtils.getIp()));
        } catch (UnknownHostException | SocketException e) {
            log.error("Check host error", e);
        }
        return false;
    }

    public String[] getDataPathByNodeId(String nodeId) {
        NodesStatsResponse resp = client.admin().cluster().prepareNodesStats(nodeId).setFs(true)
                .get();
        List<String> result = Lists.newArrayList();
        if (resp.getNodes().size() > 0) {
            for (Path path : resp.getNodes().get(0).getFs()) {
                result.add(path.getPath());
            }
        }
        return result.toArray(new String[0]);
    }


    public void triggerClusterChange(String indexName) {
        //TODO:Maybe should try a more elegant way to trigger
        boolean closeResult = client.admin().indices().prepareClose(indexName).get().isAcknowledged();
        log.info("close index: " + indexName + " is success or not:" + closeResult);
        boolean openResult = client.admin().indices().prepareOpen(indexName).get().isAcknowledged();
        log.info("open index: " + indexName + " is success or not:" + openResult);
    }

    public boolean indexExists(String indexName) {
        return client.admin().indices().prepareExists(indexName).get().isExists();
    }

    public boolean indexHealth(String indexName) {
        if (indexExists(indexName)) {
            IndicesShardStoresResponse resp = client.admin().indices()
                    .prepareShardStores(indexName).get();
            ImmutableOpenIntMap<List<StoreStatus>> shards = resp.getStoreStatuses()
                    .get(indexName);
            for (int shardId : shards.keys().toArray()) {
                for (StoreStatus storeStatus : shards.get(shardId)) {
                    if (storeStatus.getStoreException() != null) {
                        return false;
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * this method will check index exists and health
     */
    public void updateIndexSetting(String indexName, String finalIndexSetting) throws Exception {
        int waitCount = 10;
        while (!indexHealth(indexName)) {
            if (waitCount < 0) {
                throw new Exception("Wait index create and check health time out");
            }
            try {
                log.info("Wait index create and check health for 10s");
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.error("Wait index create failed", e);
            }
            waitCount--;
        }
        Map<String, String> finalIndexSettingMap = JSONObject.parseObject(finalIndexSetting, new TypeReference<Map<String, String>>() {
        });
        Settings settings = Settings.builder().putProperties(finalIndexSettingMap, new Function<String, String>() {
            @Override
            public String apply(String s) {
                return null;
            }
        }).build();
        client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).get();
    }
}