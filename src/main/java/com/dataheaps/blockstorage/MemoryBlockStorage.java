package com.dataheaps.blockstorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 20/1/17.
 */
public class MemoryBlockStorage implements BlockStorage {


    Map<Long, ByteBuffer> blocks = new HashMap<Long, ByteBuffer>();
    long maxBlockIndex = -1;
    int blockSize = 1024;

    public MemoryBlockStorage(int blockSize) {
        this.blockSize = blockSize;
    }

    public void open() throws IOException {

    }

    public void close() throws IOException {

    }

    public synchronized int getBlockSize() throws IOException {
        return blockSize;
    }

    public synchronized long getBlockCount() throws IOException {
        return maxBlockIndex + 1;
    }

    public synchronized ByteBuffer getBlock(long n) throws IOException {
        ByteBuffer block = blocks.get(n);
        if (block == null) {
            block = ByteBuffer.allocate(blockSize);
            blocks.put(n, block);
            if (n > maxBlockIndex)
                maxBlockIndex = n;
        }
        return block;
    }

    public synchronized void putBlock(long n, ByteBuffer block) throws IOException {
        blocks.put(n, block);
        if (n > maxBlockIndex)
            maxBlockIndex = n;
    }

    @Override
    public void beginTransaction() throws IOException {

    }

    @Override
    public void endTransaction() throws IOException {

    }
}
