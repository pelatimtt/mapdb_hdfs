package com.dataheaps.blockstorage;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by admin on 9/3/17.
 */
public class BlockMemoryVolumeTest {

    @org.junit.Test
    public void rwTest() throws Exception {

        File src = Files.createTempDir();

        BlockStorage bs = new LocalFileBlockStorage(128000, src, Files.createTempDir());
        //BlockStorage bs = new MemoryBlockStorage(64000);
        BlockMemory bm = new BlockMemory(bs);
        bm.open();

        DB db = DBMaker.volumeDB(new BlockMemoryVolume(bm), false).make();
        Map m = db.hashMap("test").create();

        for (int ctr=0;ctr<1000000;ctr++) {
            m.put(ctr,"testTestTest" + ctr);
            if (ctr % 20000 == 0)
                db.commit();
        }
        for (int ctr=0;ctr<1000000;ctr++) {
            assert(m.get(ctr).equals("testTestTest" + ctr));
        }

    }

    @org.junit.Test
    public void openCloseTest() throws Exception {

        File src = Files.createTempDir();

        BlockStorage bs = new LocalFileBlockStorage(64000, src, Files.createTempDir());
        BlockMemory bm = new BlockMemory(bs);
        bm.open();

        DB db = DBMaker.volumeDB(new BlockMemoryVolume(bm), false).make();
        Map m = db.hashMap("test").create();

        for (int ctr=0;ctr<150000;ctr++) {
            m.put(ctr,"test" + ctr);
            if (ctr % 2000 == 0)
                db.commit();
        }
        db.commit();
        db.close();
        bm.close();

        bs = new LocalFileBlockStorage(64000, src, Files.createTempDir());
        bm = new BlockMemory(bs);
        bm.open();

        db = DBMaker.volumeDB(new BlockMemoryVolume(bm), true).make();
        m = db.hashMap("test").open();

        for (int ctr=0;ctr<100000;ctr++) {
            assert(m.get(ctr).equals("test" + ctr));
        }

    }

    @org.junit.Test
    public void deleteTest() throws Exception {

        File src = Files.createTempDir();

        BlockStorage bs = new LocalFileBlockStorage(64000, src, Files.createTempDir());
        BlockMemory bm = new BlockMemory(bs);
        bm.open();

        DB db = DBMaker.volumeDB(new BlockMemoryVolume(bm), false).make();
        Map m = db.hashMap("test").create();

        for (int ctr=0;ctr<150000;ctr++) {
            m.put(ctr,"test" + ctr);
            if (ctr % 2000 == 0)
                db.commit();
        }

        m = db.hashMap("test2").create();

        for (int ctr=0;ctr<10000;ctr++) {
            m.put(ctr,"test" + ctr);
            if (ctr % 2000 == 0)
                db.commit();
        }

        db.commit();
        db.close();
        bm.close();

        bs = new LocalFileBlockStorage(64000, src, Files.createTempDir());
        bm = new BlockMemory(bs);
        bm.open();

        db = DBMaker.volumeDB(new BlockMemoryVolume(bm), true).make();
        m = db.hashMap("test").open();

        for (int ctr=0;ctr<150000;ctr++) {
           m.remove(ctr);
        }
        db.commit();
        db.close();
        bm.close();

        bs = new LocalFileBlockStorage(64000, src, Files.createTempDir());
        bm = new BlockMemory(bs);
        bm.open();

        db = DBMaker.volumeDB(new BlockMemoryVolume(bm), true).make();
        m = db.hashMap("test").open();

        assert(m.size() == 0);


    }

    @org.junit.Test
    public void failedTxTest() throws Exception {

        File src = Files.createTempDir();

        BlockStorage bs = new LocalFileBlockStorage(64000, src, Files.createTempDir()) {
            @Override
            public synchronized void endTransaction() throws IOException {
                FileUtils.deleteQuietly(Paths.get(sourceDir.getAbsolutePath(), "tx.started").toFile());
            }

            @Override
            public void close() throws IOException {

            }
        };

        BlockMemory bm = new BlockMemory(bs);
        bm.open();

        DB db = DBMaker.volumeDB(new BlockMemoryVolume(bm), false).make();
        Map m = db.hashMap("test").create();

        for (int ctr=0;ctr<100000;ctr++) {
            m.put(ctr,"test" + ctr);
        }
        db.commit();
        db.close();
        bm.close();

        bs = new LocalFileBlockStorage(64000, src, Files.createTempDir());
        bm = new BlockMemory(bs);
        bm.open();

        db = DBMaker.volumeDB(new BlockMemoryVolume(bm), true).make();
        m = db.hashMap("test").open();

        for (int ctr=0;ctr<100000;ctr++) {
            assert(m.get(ctr).equals("test" + ctr));
        }

    }

}