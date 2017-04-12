package com.dataheaps.blockstorage.factories;

import com.dataheaps.blockstorage.BlockStorage;

/**
 * Created by admin on 12/4/17.
 */
public interface StorageFactory {
    BlockStorage create();
    void init() throws Exception;
}
