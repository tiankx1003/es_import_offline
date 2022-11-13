package io.github.tiankx1003.utils;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-07 10:19
 */
@Slf4j
public class Utils {

    public static String EMAIL_API_URL = "";

    public static void unzip(Path src, Path dest) throws IOException {
        try (
                ZipArchiveInputStream in = new ZipArchiveInputStream(
                        new BufferedInputStream(Files.newInputStream(src)))
        ) {
            ArchiveEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                if (!in.canReadEntryData(entry)) {
                    // log something?
                    continue;
                }

                File f = dest.resolve(entry.getName()).toFile();
                if (entry.isDirectory()) {
                    if (!f.isDirectory() && !f.mkdirs()) {
                        throw new IOException("failed to create directory " + f);
                    }
                } else {
                    File parent = f.getParentFile();
                    if (!parent.isDirectory() && !parent.mkdirs()) {
                        throw new IOException("failed to create directory " + parent);
                    }
                    try (OutputStream o = new BufferedOutputStream(Files.newOutputStream(f.toPath()))) {
                        IOUtils.copy(in, o);
                    }
                }
            }
        }
    }

    /**
     * 获取剩余空间最大的目录
     */
    public static String mostFreeDir(String[] paths) {
        String result = paths[0];
        long mostFree = 0L;
        for (String path : paths) {
            File destPath = new File(path);
            if (!destPath.exists()) {
                log.info("path not exists, skip this path: " + path);
            } else {
                long freeSpace = new File(path).getFreeSpace();
                if (freeSpace > mostFree) {
                    mostFree = freeSpace;
                    result = path;
                }
            }
        }
        return result;
    }

    public static String mostFreeDir(Set<String> paths) {
        String result = null;
        long mostFree = 0L;
        for (String path : paths) {
            long freeSpace = new File(path).getFreeSpace();
            if (freeSpace > mostFree) {
                mostFree = freeSpace;
                result = path;
            }
        }
        return result;
    }

    public static String mostFreeDir(String[] paths, Set<String> chosenPaths) {
        Set<String> candidatePaths = Sets.newHashSet(paths);
        candidatePaths.removeAll(chosenPaths);
        String result = mostFreeDir(candidatePaths);
        if (result == null) {
            result = mostFreeDir(chosenPaths);
        }
        return result;
    }

    /**
     * 从dirs中选择一个和referenceDir在同一块磁盘的目录 如果没有在同一块磁盘上的则选择剩余空间最大的
     */
    public static String sameDiskDir(String[] dirs, String referenceDir) {
        try {
            String refFileStore = getFileStore(referenceDir);
            for (String dir : dirs) {
                File destPath = new File(dir);
                if (!destPath.exists()) {
                    log.info("path not exists,skip it: " + dir);
                } else {
                    String fileStore = getFileStore(dir);
                    if (fileStore.equalsIgnoreCase(refFileStore)) {
                        return dir;
                    }
                }
            }
        } catch (IOException e) {
            log.error("Get file store error", e);
        }
        log.warn("No same disk, use most free disk");
        return mostFreeDir(dirs);
    }

    /**
     * 获取path所在的mountPoint
     */
    public static String mountPoint(String path) throws IOException {
        FileStore store = Files.getFileStore(Paths.get(path));
        return getMountPointLinux(store);
    }

    public static String getFileStore(String path) throws IOException {
        return Files.getFileStore(Paths.get(path)).name();
    }

    private static String getMountPointLinux(FileStore store) {
        String desc = store.toString();
        int index = desc.lastIndexOf(" (");
        if (index != -1) {
            return desc.substring(0, index);
        } else {
            return desc;
        }
    }

    public static int chownDire(String path, String newOwner) throws IOException, InterruptedException {
        String cmdLine = "sudo chown -R " + newOwner + ":" + newOwner + " " + path;
        return execCmdLine(cmdLine);
    }

    public static int setPermissionRecursive(Path path) throws IOException, InterruptedException {
        String cmdLine = "sudo chmod -R 777 " + path.toString();
        return execCmdLine(cmdLine);

    }

    public static int forceCP(String from, String to) throws IOException, InterruptedException {
        String cmdLine = "sudo cp -r " + from + " " + to;
        return execCmdLine(cmdLine);
    }

    public static int downloadHdfsFile(String path) throws IOException, InterruptedException {
        String cmdLine = "hdfs dfs -get " + path;
        return execCmdLine(cmdLine);
    }

    public static int deleteDir(String dirName) throws IOException, InterruptedException {
        String cmdLine = "sudo rm -rf " + dirName;
        return execCmdLine(cmdLine);
    }

    public static int execCmdLine(String cmdLine) throws InterruptedException, IOException {
        Process exec = Runtime.getRuntime().exec(cmdLine);
        int result = exec.waitFor();
        log.info("exec: " + cmdLine + " result: " + result);
        exec.destroy();
        return result;
    }

    public static String getBeforeIndexName(String indexName, int dateDistance, String splitTag) {
        String[] split = indexName.split(splitTag);
        String latestDate = split[split.length - 1];
        String needDate = "";
        StringBuilder oldIndexName = new StringBuilder();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        try {
            Date parse = dateFormat.parse(latestDate);
            Calendar calendar = new GregorianCalendar();
            calendar.setTime(parse);
            calendar.add(Calendar.DATE, dateDistance);
            needDate = dateFormat.format(calendar.getTime());
        } catch (ParseException e) {
            log.error("get old date failed:" + e);
        }

        for (int j = 0; j < split.length - 1; j++) {
            oldIndexName.append(split[j]).append(splitTag);
        }
        oldIndexName.append(needDate);
        log.info("need delete indexName:" + oldIndexName);

        return oldIndexName.toString();
    }


    /**
     * send email notification
     *
     * @param toWho address
     * @param sub   subtitle
     * @param cont  content
     */
    public static void sendEmail(String toWho, String sub, String cont) {
        List<NameValuePair> params = new ArrayList<>();
        BasicNameValuePair to = new BasicNameValuePair("to", toWho);
        BasicNameValuePair subject = new BasicNameValuePair("subject", sub);
        BasicNameValuePair content = new BasicNameValuePair("content", cont);

        params.add(to);
        params.add(subject);
        params.add(content);

        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost(EMAIL_API_URL);

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            CloseableHttpResponse response = client.execute(httpPost);
            String result;
            // 获得字符串形式的结果
            result = EntityUtils.toString(response.getEntity());
            log.info("send email result:" + result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
