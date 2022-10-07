package io.github.tiankx1003.utils;

import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @version 1.0
 * @since 2022-10-02 12:28
 */

public class CompressionUtils {

    private static final Log log = LogFactory.getLog(CompressionUtils.class);

    public static void upload2HDFS(String from, String to) {
        try {
            FileInputStream fileInputStream = new FileInputStream(from);
            BufferedInputStream in = new BufferedInputStream(fileInputStream);

            // /tmp/es/custom_test_20191215
            Path toPath = new Path(to);
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if (!fileSystem.exists(toPath.getParent())) {
                log.info(String.format("hdfs path %s not exist and create it", toPath.getParent()));
                fileSystem.mkdirs(toPath.getParent());
            }
            FSDataOutputStream out = fileSystem.create(toPath);
            IOUtils.copy(in, out);
            in.close();
            out.hflush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void zipAndUpload(String from, String to, String rootDirName, FileSystem fs) throws IOException {
        log.info("ZipAndUpload from [" + from + "] to [" + to + "] rootDirName : [" + rootDirName + "]");
        java.nio.file.Path fromPath = Paths.get(from);
        Path toPath = new Path(to);

        if (!fs.exists(toPath.getParent())) {
            log.info(String.format("hdfs path %s not exist and create it", toPath.getParent()));
            fs.mkdirs(toPath.getParent());
        }
        Path tmpToPath = new Path(to + "_tmp");

        try (FSDataOutputStream fsDataOutputStream = fs.create(tmpToPath, true, 1024 * 1024); ZipArchiveOutputStream out = new ZipArchiveOutputStream(fsDataOutputStream)) {
            out.setUseZip64(Zip64Mode.AsNeeded);
            Files.walk(fromPath, Integer.MAX_VALUE).filter(path -> !Files.isDirectory(path)).forEach(path -> {
                ZipArchiveEntry zipEntry = new ZipArchiveEntry(rootDirName + "/" + fromPath.relativize(path));
                try {
                    out.putArchiveEntry(zipEntry);
                    IOUtils.copy(Files.newInputStream(path), out);
                    out.closeArchiveEntry();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        fs.rename(tmpToPath, toPath);
    }

}

