package com.dataheaps.blockstorage.factories;

import com.dataheaps.blockstorage.BlockStorage;
import com.dataheaps.blockstorage.LocalFileBlockStorage;
import com.google.common.io.Files;

import java.io.File;

/**
 * Created by admin on 12/4/17.
 */
public class LocalFileStorageFactory implements StorageFactory {

    File src = Files.createTempDir();

    @Override
    public void init() throws Exception {

    }

    @Override
    public BlockStorage create() {
        return new LocalFileBlockStorage(1024*64, src, Files.createTempDir());
    }

}
