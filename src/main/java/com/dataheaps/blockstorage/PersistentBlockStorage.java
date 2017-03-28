package com.dataheaps.blockstorage;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by admin on 9/3/17.
 */

@RequiredArgsConstructor
public abstract class PersistentBlockStorage implements BlockStorage {

    @AllArgsConstructor
    static class FileInfo {
        File fd;
        RandomAccessFile file;
        FileChannel channel;
        ByteBuffer buffer;
    }


    final int blockSize;

    Map<Long, FileInfo> blocks = new HashMap<Long, FileInfo>();

    protected abstract int getBlocksCount() throws IOException;
    protected abstract File getBlockFile(long id) throws IOException;
    protected abstract void putBlockFile(long id, File f) throws IOException;

    public void open() throws IOException {

    }

    public void close() throws IOException {

    }

    public synchronized int getBlockSize() throws IOException {
        return blockSize;
    }

    public synchronized long getBlockCount() throws IOException {
        return getBlocksCount();
    }

    public synchronized ByteBuffer getBlock(long n) throws IOException {
        File f = getBlockFile(n);
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        FileChannel channel = raf.getChannel();
        ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, blockSize);
        blocks.put(n, new FileInfo(f, raf, channel, buffer));
        return buffer;
    }

    public synchronized void putBlock(long n, ByteBuffer block) throws IOException {
        FileInfo f = blocks.get(n);
        f.channel.force(true);
        f.channel.close();
        f.file.close();
        putBlockFile(n, f.fd);
        blocks.remove(n);
    }

    @Override
    public void beginTransaction() throws IOException {

    }

    @Override
    public void endTransaction() throws IOException {

    }
}
