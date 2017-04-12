package com.dataheaps.blockstorage;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by admin on 12/4/17.
 */
public class ZookeeperBlockStorageTest {

    @Test
    public void connect() throws Exception {

        TestingServer server = new TestingServer(44812);
        server.start();

        ZookeeperBlockStorage bs = new ZookeeperBlockStorage(server.getConnectString(), Paths.get("/db_test"));
        BlockMemory bm = new BlockMemory(bs);
        bm.open();

        DB db = DBMaker.volumeDB(new BlockMemoryVolume(bm), false).make();
        Map m = db.hashMap("test").createOrOpen();

        long t0 = System.currentTimeMillis();
        for (int ctr=0;ctr<150000;ctr++) {
            m.put(ctr,"test" + ctr);
            if (ctr % 2000 == 0) {
                db.commit();
            }
        }
        db.commit();
        db.close();

        System.out.println(System.currentTimeMillis() - t0);

        bs = new ZookeeperBlockStorage(server.getConnectString(), Paths.get("/db_test"));
        bm = new BlockMemory(bs);
        bm.open();

        db = DBMaker.volumeDB(new BlockMemoryVolume(bm), true).make();
        m = db.hashMap("test").open();

        for (int ctr=0;ctr<100000;ctr++) {
            assert(m.get(ctr).equals("test" + ctr));
        }


    }

    @org.junit.Test
    public void failedTxTest() throws Exception {

        TestingServer server = new TestingServer(44812);
        server.start();


        BlockStorage bs = new ZookeeperBlockStorage(server.getConnectString(), Paths.get("/db_test")) {
            @Override
            public void close() throws IOException {
                System.out.print("closing");
            }

            @Override
            public synchronized void endTransaction() throws IOException {
                try {
                    client.setData().forPath(Paths.get(zkPath.toString(), TX_IN_PROGRESS_PATH).toString(), FALSE_BYTE);
                }
                catch (Exception e) {
                    throw new IOException(e);
                }
            }
        };
        BlockMemory bm = new BlockMemory(bs);
        bm.open();
        DB db = DBMaker.volumeDB(new BlockMemoryVolume(bm), false).make();
        Map m = db.hashMap("test").createOrOpen();

        for (int ctr=0;ctr<100000;ctr++) {
            m.put(ctr,"test" + ctr);
        }
        db.commit();
        db.close();


        bs = new ZookeeperBlockStorage(server.getConnectString(), Paths.get("/db_test"));
        bm = new BlockMemory(bs);
        bm.open();

        db = DBMaker.volumeDB(new BlockMemoryVolume(bm), true).make();
        m = db.hashMap("test").open();

        for (int ctr=0;ctr<100000;ctr++) {
            assert(m.get(ctr).equals("test" + ctr));
        }

    }


}