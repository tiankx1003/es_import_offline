package io.github.tiankx1003;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.github.tiankx1003.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.index.translog.Translog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:26
 */
@Slf4j
@Service
public class IndexBuilder {

    @Value("#{'${workDir}'.split(',')}")
    private String[] workDirs;

    @Autowired
    private HdfsClient hdfsClient;

    @Autowired
    private ESClient esClient;

    private static final String STATE_DIR = "_state";

    private Map<String, Boolean> completedIndices = Maps.newConcurrentMap();

    private String INDEX_FILE = "index";

    private String TLOG_UUID = "";

    private String TRANSLOG_PATH = "translog";

    private String TRANSLOG_TLOG_FILE = "translog-1.tlog";

    private String TRANSLOG_GENERATION_VALUE = "1";

    public boolean build(Map<String, List<String>> idToShards, JSONObject configData) {
        String hdfsWorkDir = configData.getString("hdfsWorkDir");
        String indexName = configData.getString("indexName");
        log.info("start build: " + indexName + ":" + hdfsWorkDir);
        //TODO no need localstatdir
        Path localStateDir = Paths.get(Utils.mostFreeDir(workDirs), indexName);
        if (downloadAndMergeAllShards(idToShards, hdfsWorkDir, indexName, localStateDir)) {
            try {
                FileUtils.deleteDirectory(localStateDir.toFile());
                log.info("Delete state file {}", localStateDir.resolve(STATE_DIR));
                completedIndices.remove(indexName);
            } catch (Exception e) {
                log.error("delete state file error", e);
            }
            return true;
        }
        return false;
    }

    private boolean downloadAndMergeAllShards(Map<String, List<String>> idToShards,
                                              String hdfsWorkDir, String indexName, Path localStateDir) {
//      TODO 这里要开两个线程跑，太慢
        idToShards.entrySet().forEach(entry -> {
            log.info("start : " + entry);
            String nodeId = entry.getKey();
            List<String> shards = entry.getValue();
            downloadAndMergeByNode(nodeId, shards, hdfsWorkDir, indexName);
            log.info("build: " + nodeId + " shards: " + shards+" finished");
        });
        log.info("finish build: " + idToShards);
        return true;
    }

    //don't clean tmp dir hear, use 32's shell clean by crontable
    private void downloadAndMergeByNode(String nodeId, List<String> shards,
                                        String hdfsWorkDir,
                                        String indexName) {
        log.info("build assion" + nodeId + " shards: " + shards);
        for (String shardId : shards) {
            log.info(indexName + ":" + shardId + "-" + Integer.getInteger(shardId));
            String dataPath = esClient.getShardDataPath(indexName, new Integer(shardId));
            log.info("es data dir is {}", dataPath);
            String srcPath = Paths.get(hdfsWorkDir, indexName, shardId).toString();
            log.info("hdfs path is: " + srcPath);
            String workDir = Utils.sameDiskDir(workDirs, dataPath);
            String destPath = Paths.get(workDir, indexName, shardId).toString();
            log.info("Chosen tmpwork dir is {}", destPath);
            try {
                downloadAndUnzipShard(srcPath, destPath, indexName);
                log.info("Merge index bundle in dir[{}] ", destPath);
                String finalIndexPath = mergeIndex(destPath);
                if (finalIndexPath.equals("1")) {
                    log.info("no segfile need deal, finish this node job");
                } else {
                    moveLuceneToESDataDir(indexName, shardId, dataPath, finalIndexPath);
                }
            } catch (IOException e) {
                log.error(
                        "Build index bundle from hdfs[" + srcPath + "] failed", e);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void downloadAndUnzipShard(String srcPath, String destPath, String indexName) {
        log.info("Download and unzip index bundle from hdfs[{}] to local[{}] start", srcPath, destPath);
        Set<String> processedPaths = Sets.newHashSet();
        File dstDir = new File(destPath);
        if (!dstDir.exists()) {
            log.info("Dest path not exists and create it {}", destPath);
            dstDir.mkdirs();
        }
        while (true) {
            List<String> paths = hdfsClient.listCompletedFiles(srcPath);
            paths.removeAll(processedPaths);
            if (paths.size() == 0) {
                if (indexCompleted(indexName)) {
                    log.info("Index completed and all partition submitted");
                    break;
                } else {
                    log.info("Wait index complete for 10s...");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        log.info("Wait index complete error", e);
                    }
                }
            } else {
                log.info("Got new path size : {} and processed path size:{}", paths.size(),
                        processedPaths.size());
                paths.forEach(srcFile -> {
                    String fileName = srcFile.substring(srcFile.lastIndexOf('/') + 1);
                    String from = srcPath + '/' + fileName;
                    String to = destPath;
                    submitDownloadAndUnzipShardPartitionTask(from, to);
                });
                processedPaths.addAll(paths);
            }
        }
        log.info("Download and unzip index bundle from hdfs[{}] to local[{}] end", srcPath, destPath);
    }

    private void submitDownloadAndUnzipShardPartitionTask(String srcPath, String destPath) {
        log.info("Submit download and unzip task from {} to {}", srcPath, destPath);
        try {
            hdfsClient.downloadAndUnzipFile(srcPath, destPath);
        } catch (IOException e) {
            log.info("Submit download and unzip task from {} to {} eorro", srcPath, destPath);
            e.printStackTrace();
        }
    }

    //改成之移动lucene文件,并更改lucene的user data
    private void moveLuceneToESDataDir(String indexName, String shardId,
                                       String dataPath, String finalIndexPath) throws IOException, InterruptedException {
        Path from = Paths.get(finalIndexPath + "/" + INDEX_FILE);
        Path to = Paths.get(dataPath, "indices", indexName, shardId, INDEX_FILE);
        Utils.setPermissionRecursive(to.getParent());
        Utils.setPermissionRecursive(from.getParent());

        Utils.deleteDir(to.toString());
        boolean isMoveFailed = true;
        int retryTimes = 5;
        for (int i = 0; i < retryTimes; i++) {
            try {
                Files.move(from, to);
                isMoveFailed = false;
                log.info("move file from:" + from.toString() + " to:" + to.toString() + " success");
                break;
            } catch (Exception e) {
                log.info("moveLuceneFile failed: " + e.getMessage());
                Utils.setPermissionRecursive(to.getParent());
                Utils.deleteDir(to.toString());
            }
        }
        if (isMoveFailed) {
            File file = new File(to.toString());
            File[] files = file.listFiles();
            log.info("move file from:" + from.toString() + " to:" + to.toString());

            Utils.forceCP(from.toString(), to.getParent().toString());
            for (File segFile : files) {
                log.info("delete file:" + segFile.getPath());
                Files.delete(segFile.toPath());
            }
        }

        Utils.setPermissionRecursive(to.getParent().getParent());
        //TODO:once get null from tlog file, get the es version ,and then use Strings.randomBase64UUID() create .tlog and .ckp file
        if (getSegInfo(to.getParent().resolve(TRANSLOG_PATH).resolve(TRANSLOG_TLOG_FILE).toString())) {
            setToLucene(to);
        }
    }

    private boolean getSegInfo(String tlog) throws IOException {
        log.info("getSeginfo: " + tlog);
        //TODO: ensure no flush
        File file = new File(tlog);
        while (!file.exists()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("translog file don't exists, wait 3s");
        }
        FileInputStream fileInputStream = new FileInputStream(file);
        log.info(file.getPath().toString());
        byte[] bytes = new byte[1024];
        fileInputStream.read(bytes);
        String uuid = new String(bytes, 20, 43).trim();
        log.info("uuid: " + uuid);
        TLOG_UUID = uuid;
        log.info("get tlog file: " + tlog + " uuid: " + TLOG_UUID);
        return true;
    }

    private void setToLucene(Path segPath) {
        log.info("set to lucene: " + segPath);
        try {
            FSDirectory directory = FSDirectory.open(segPath);
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
            Map<String, String> commitData = new HashMap<>(2);
            commitData.put(Translog.TRANSLOG_GENERATION_KEY, TRANSLOG_GENERATION_VALUE);
            commitData.put(Translog.TRANSLOG_UUID_KEY, TLOG_UUID);

            Method setUserData = segmentInfos.getClass().getDeclaredMethod("setUserData", Map.class);
            setUserData.setAccessible(true);
            setUserData.invoke(segmentInfos, commitData);

            Method commit = segmentInfos.getClass().getDeclaredMethod("commit", Directory.class);
            commit.setAccessible(true);
            commit.invoke(segmentInfos, directory);

            log.info("set userData: " + segmentInfos.getUserData());

            Utils.setPermissionRecursive(segPath.getParent());
            Utils.chownDire(segPath.getParent().toString(), "elasticsearch");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用Lucene合并索引会非常慢，所以这里直接进行文件移动，然后重新生成segment信息
     */
    private String mergeIndex(String indexBundlePath) throws IOException {
        File parentFile = new File(indexBundlePath);
        log.info("get parentFile suceecss:" + parentFile.getAbsolutePath() + "-" + indexBundlePath);
        File[] containFiles = parentFile.listFiles();
        log.info("num of files " + containFiles.length);
        if (containFiles.length == 0) {
            return "1";
        }
        List<Path> indexList = new ArrayList<>();
        for (File shardFile : containFiles) {
            if (shardFile.isDirectory()) {
                indexList.add(shardFile.toPath());
            }
        }
        log.info("indexList: " + indexList + " size: " + indexList.size());
        Collections.sort(indexList);
        try (
                FSDirectory directory = FSDirectory.open(indexList.get(0).resolve("index"))
        ) {
            log.info("Original segment info file path is {}", indexList.get(0).resolve("index").toString());
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
            String originSegmentFileName = segmentInfos.getSegmentsFileName();
            Path originSegmentPath = directory.getDirectory().resolve(originSegmentFileName);
            log.info("Original segment info file path is {}", originSegmentPath.toString());
            List<SegmentCommitInfo> infos = new ArrayList<>();

            for (int i = 1; i < indexList.size(); i++) {
                FSDirectory dir = FSDirectory.open(indexList.get(i).resolve("index"));
                SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
                for (SegmentCommitInfo info : sis) {
                    String newSegName = newSegmentName(segmentInfos);
                    infos.add(copySegmentAsIs(directory, info, newSegName));
                }
            }
            segmentInfos.addAll(infos);
            SegmentInfos pendingCommit = segmentInfos.clone();
            Method prepareCommit = pendingCommit.getClass()
                    .getDeclaredMethod("prepareCommit", Directory.class);
            prepareCommit.setAccessible(true);
            prepareCommit.invoke(pendingCommit, directory);

            log.info("Add pending segment info file");

            Method updateGeneration = segmentInfos.getClass()
                    .getDeclaredMethod("updateGeneration", SegmentInfos.class);
            updateGeneration.setAccessible(true);
            updateGeneration.invoke(segmentInfos, pendingCommit);

            log.info("Update segment info generation");

            Method finishCommit = pendingCommit.getClass()
                    .getDeclaredMethod("finishCommit", Directory.class);
            finishCommit.setAccessible(true);
            finishCommit.invoke(pendingCommit, directory);

            log.info("Finish segment info commit");

            Files.delete(originSegmentPath);
            log.info("Delete origin segment info file {}", originSegmentPath.toString());

            log.info("merge index for shard " + indexBundlePath + " done");
        } catch (IOException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            log.error("Merge index for shard " + indexBundlePath + " error", e);
        }
        return indexList.get(0).toString();
    }

    private SegmentCommitInfo copySegmentAsIs(Directory directory, SegmentCommitInfo info,
                                              String segName) throws IOException {

        SegmentInfo newInfo = new SegmentInfo(directory, info.info.getVersion(), segName,
                info.info.maxDoc(),
                info.info.getUseCompoundFile(), info.info.getCodec(),
                info.info.getDiagnostics(), info.info.getId(), info.info.getAttributes());
        SegmentCommitInfo newInfoPerCommit = new SegmentCommitInfo(newInfo, info.getDelCount(),
                info.getDelGen(),
                info.getFieldInfosGen(), info.getDocValuesGen());

        newInfo.setFiles(info.files());

        boolean success = false;

        Set<String> copiedFiles = new HashSet<>();
        try {
            // Copy the segment's files
            for (String file : info.files()) {
                Method namedForThisSegment = newInfo.getClass()
                        .getDeclaredMethod("namedForThisSegment", String.class);
                namedForThisSegment.setAccessible(true);
                final String newFileName = (String) namedForThisSegment.invoke(newInfo, file);

                FSDirectory srcDir = (FSDirectory) info.info.dir;
                FSDirectory destDir = (FSDirectory) directory;
                Path srcFile = srcDir.getDirectory().resolve(file);
                Path destFile = destDir.getDirectory().resolve(newFileName);
                Files.move(srcFile, destFile);
                log.debug("Move index file from {} to {}", srcFile.toString(), destFile.toString());
                copiedFiles.add(newFileName);
            }
            success = true;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            log.error("Get new segment name error", e);
        } finally {
            if (!success) {
//        deleteNewFiles(copiedFiles);
            }
        }

        assert copiedFiles.equals(newInfoPerCommit.files());

        return newInfoPerCommit;
    }

    public String newSegmentName(SegmentInfos segmentInfos) {
        segmentInfos.changed();
        return "_" + Integer.toString(segmentInfos.counter++, Character.MAX_RADIX);
    }


    public void markIndexCompleted(String indexName) {
        completedIndices.put(indexName, true);
    }

    public boolean indexCompleted(String indexName) {
        return completedIndices.getOrDefault(indexName, false);
    }
}