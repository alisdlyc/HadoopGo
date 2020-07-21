package com.alisdlyc.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author alisdlyc
 */
public class HDFSClient {

    private static FileSystem fs;
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        try {
            fs = getFs();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }

        System.out.println("OVO");
    }

    private static FileSystem getFs() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        return FileSystem.get(new URI("hdfs://127.0.0.1:9000"), conf, "alisdlyc");
    }

    @Test
    public void testCopyFromLocalFile() throws IOException {
        try {
            fs = getFs();
            // form database to hadoop qwq
            fs.copyFromLocalFile(new Path("/home/alisdlyc/qwq/qwq.txt"), new Path("/user/alisdlyc/qwq/ovo.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }

    @Test
    public void testCopyToLocalFile() throws IOException {
        try {
            fs = getFs();
            fs.copyToLocalFile(new Path("/user/alisdlyc/qwq/ovo.txt"), new Path("/home/alisdlyc/qwq/ovo.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }

    @Test
    public void testDelete() throws IOException {
        try {
            fs = getFs();
            fs.delete(new Path("/user/alisdlyc/qwq/qwq.txt"), false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }

    @Test
    public void testRename() throws IOException {
        try {
            fs = getFs();
            fs.rename(new Path("/user/alisdlyc/qwq/ovo.txt"), new Path("/user/alisdlyc/qwq/qwq.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }

    @Test
    public void testListFiles() throws IOException {
        try {
            fs = getFs();
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/user/alisdlyc"), true);
            while (listFiles.hasNext()){
                LocatedFileStatus fileStatus = listFiles.next();

                System.out.println(fileStatus.getPath().getName());
                System.out.println(fileStatus.getPermission());
                System.out.println(fileStatus.getLen());
                System.out.println(Arrays.toString(fileStatus.getBlockLocations()));
                System.out.println("----------------------------");
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }

    @Test
    public void testListStatus() throws IOException {
        try {
            fs = getFs();
            FileStatus[] listStatus = fs.listStatus(new Path("/"));

            for (FileStatus fileStatus : listStatus) {
                if (fileStatus.isFile()) {
                    System.out.println("f: " + fileStatus.getPath().getName());
                } else {
                    System.out.println("d: " + fileStatus.getPath().getName());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }
}
