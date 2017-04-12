package com.dataheaps.blockstorage.factories;

import com.dataheaps.blockstorage.BlockStorage;
import com.dataheaps.blockstorage.ZookeeperBlockStorage;
import org.apache.curator.test.TestingServer;

import java.nio.file.Paths;

/**
 * Created by admin on 12/4/17.
 */
public class ZookeeperStorageFactory implements StorageFactory {

    TestingServer ts;

    @Override
    public void init() throws Exception {
        ts = new TestingServer(44567);
        ts.start();
    }

    @Override
    public BlockStorage create() {
        return new ZookeeperBlockStorage(ts.getConnectString(), Paths.get("/db_test"));
    }
}
