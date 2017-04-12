package com.dataheaps.blockstorage;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by admin on 27/3/17.
 */
public class HdfsBlockStorageTest {

    @Test
    public void connect() throws Exception {

        HdfsLocalCluster hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(12345)
                .setHdfsNamenodeHttpPort(12346)
                .setHdfsTempDir("embedded_hdfs")
                .setHdfsNumDatanodes(1)
                .setHdfsEnablePermissions(false)
                .setHdfsFormat(true)
                .setHdfsEnableRunningUserAsProxyUser(true)
                .setHdfsConfig(new Configuration())
                .build();
        hdfsLocalCluster.start();

        FileSystem fs = FileSystem.get(hdfsLocalCluster.getHdfsConfig());

        File src = Files.createTempDir();

        BlockStorage bs = new HdfsBlockStorage(64000*10, fs, "/test", Files.createTempDir());
      //  BlockStorage bs = new LocalFileBlockStorage(64000*10, Files.createTempDir(), Files.createTempDir());
        BlockMemory bm = new BlockMemory(bs);
        bm.open();

        DB db = DBMaker.volumeDB(new BlockMemoryVolume(bm), false).make();
     //   DB db = DBMaker.tempFileDB().make();
        Map m = db.hashMap("test").create();

        long t0 = System.currentTimeMillis();
        for (int ctr=0;ctr<150000;ctr++) {
            m.put(ctr,"test" + ctr);
            if (ctr % 20000 == 0)
                db.commit();
        }
        db.commit();
        db.close();
        bm.close();

        System.out.println(System.currentTimeMillis() - t0);

//        bs = new HdfsBlockStorage(64000*10, fs, "/test", Files.createTempDir());
//        bm = new BlockMemory(bs);
//        bm.open();
//
//        db = DBMaker.volumeDB(new BlockMemoryVolume(bm), true).make();
//        m = db.hashMap("test").open();
//
//        for (int ctr=0;ctr<100000;ctr++) {
//            assert(m.get(ctr).equals("test" + ctr));
//        }

    }

}