package com.alisdlyc.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author alisdlyc
 */
public class HDFSIO {
    private static FileSystem fs;

    public static void main(String[] args) {

    }

    private static FileSystem getFs() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        return FileSystem.get(new URI("hdfs://127.0.0.1:9000"), conf, "alisdlyc");
    }

    @Test
    public void putFileToHdfs() throws InterruptedException, IOException, URISyntaxException {
        fs = getFs();

        FileInputStream fis = new FileInputStream(new File("/home/alisdlyc/qwq/ovo.txt"));

        FSDataOutputStream fos = fs.create(new Path("/user/alisdlyc/qwq/ovo.txt"));

        IOUtils.copyBytes(fis, fos, new Configuration());

        IOUtils.closeStreams(fos, fis);

        fs.close();

    }

    @Test
    public void getFileFromHdfs() throws InterruptedException, IOException, URISyntaxException {
        fs = getFs();

        FSDataInputStream fis = fs.open(new Path("/user/alisdlyc/qwq/ovo.txt"));

        FileOutputStream fos = new FileOutputStream(new File("/home/alisdlyc/qwq/QWQ.txt"));

        IOUtils.copyBytes(fis, fos, new Configuration());

        IOUtils.closeStreams(fos, fis);
        fs.close();

    }

    @Test
    public void readFileSeek1() throws InterruptedException, IOException, URISyntaxException {

        // just copy 128MB
        fs = getFs();

        FSDataInputStream fis = fs.open(new Path("/user/alisdlyc/qwq/anchor.mp4"));

        FileOutputStream fos = new FileOutputStream(new File("/home/alisdlyc/qwq/xi.mp4.part1"));

        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            int read = fis.read(buf);
            fos.write(buf);
        }

        IOUtils.closeStreams(fos, fis);
        fs.close();

    }

    @Test
    public void readFileSeek2() throws InterruptedException, IOException, URISyntaxException {

        // copy the another 128MB
        fs = getFs();

        FSDataInputStream fis = fs.open(new Path("/user/alisdlyc/qwq/anchor.mp4"));
        // set a start seek
        fis.seek(1024 * 1024 * 128);

        FileOutputStream fos = new FileOutputStream(new File("/home/alisdlyc/qwq/xi.mp4.part2"));

        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            int read = fis.read(buf);
            fos.write(buf);
        }

        IOUtils.closeStreams(fos, fis);
        fs.close();

    }
}
