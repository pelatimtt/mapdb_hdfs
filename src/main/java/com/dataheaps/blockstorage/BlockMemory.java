package com.dataheaps.blockstorage;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by admin on 20/1/17.
 */

@RequiredArgsConstructor
public class BlockMemory {

    final BlockStorage storage;
    Map<Long, ByteBuffer> cachedBlocks = new HashMap<>();
    Set<Long> dirtyBlocks = new HashSet<>();

    BlockPosition getBlock(long pos) throws IOException {
        return new BlockPosition(
                new Double(Math.floor(new Long(pos).doubleValue() / new Long(storage.getBlockSize()).doubleValue())).longValue(),
                (int) (pos % storage.getBlockSize())
        );
    }

    ByteBuffer getBlockBuffer(long pos) throws IOException {
        ByteBuffer buffer = cachedBlocks.get(pos);
        if (buffer == null) {
            buffer = storage.getBlock(pos);
            cachedBlocks.put(pos, buffer);
        }
        return buffer;
    }

    public synchronized void read(long pos, byte[] buffer, int bufferLen) throws IOException {

        BlockPosition blockInfo = getBlock(pos);
        int totalOffset = 0;
        long block = blockInfo.getBlock();
        int offset = blockInfo.offset;
        while (totalOffset < bufferLen) {
            ByteBuffer b = getBlockBuffer(block);
            b.position(offset);
            int length = b.remaining() > (bufferLen - totalOffset) ? (bufferLen - totalOffset) : b.remaining();
            b.get(buffer, totalOffset, length);
            totalOffset += length;
            block++;
            offset = 0;
        }

    }

    public synchronized void read(long pos, ByteBuffer dest) throws IOException {

        BlockPosition blockInfo = getBlock(pos);
        long block = blockInfo.getBlock();
        int offset = blockInfo.offset;

        while (dest.remaining() > 0) {
            ByteBuffer src = getBlockBuffer(block);
            src.position(offset);

            if (dest.remaining() >= src.remaining()) {
                dest.put(src);
            }
            else {
                ByteBuffer cloned = src.duplicate();
                cloned.position(src.position());
                cloned.limit(src.position() + dest.remaining());
                src.position(src.position() + dest.remaining());
                dest.put(cloned);
            }
            block++;
            offset = 0;
        }

    }

    public synchronized void write(long pos, byte[] buffer, int bufferLen) throws IOException {

        BlockPosition blockInfo = getBlock(pos);
        int totalOffset = 0;
        long block = blockInfo.getBlock();
        int offset = blockInfo.offset;
        while (totalOffset < bufferLen) {
            dirtyBlocks.add(block);
            ByteBuffer b = getBlockBuffer(block);
            b.position(offset);
            int length = b.remaining() > (bufferLen - totalOffset) ? (bufferLen - totalOffset) : b.remaining();
            b.put(buffer, totalOffset, length);
            totalOffset += length;
            block++;
            offset = 0;
        }

    }

    public synchronized void write(long pos, ByteBuffer src) throws IOException {

        BlockPosition blockInfo = getBlock(pos);
        long block = blockInfo.getBlock();
        int offset = blockInfo.offset;

        while (src.remaining() > 0) {
            dirtyBlocks.add(block);
            ByteBuffer dest = getBlockBuffer(block);
            dest.position(offset);

            if (dest.remaining() >= src.remaining()) {
                dest.put(src);
            }
            else {
                ByteBuffer cloned = src.duplicate();
                cloned.position(src.position());
                cloned.limit(src.position() + dest.remaining());
                src.position(src.position() + dest.remaining());
                dest.put(cloned);
            }
            block++;
            offset = 0;
        }
    }

    public synchronized void clear(long pos, int count) throws IOException {

        BlockPosition blockInfo = getBlock(pos);
        int currPos = 0;
        long block = blockInfo.getBlock();
        int offset = blockInfo.offset;

        while (currPos < count) {
            dirtyBlocks.add(block);
            ByteBuffer b = getBlockBuffer(block);
            b.position(offset);
            while (b.remaining() > 0) {
                b.put((byte) 0);
                currPos++;
            }
            block++;
            offset = 0;
        }

    }

    public synchronized void close() throws IOException {
        commit();
        storage.close();
    }

    public synchronized void open() throws IOException {
        storage.open();
    }

    public synchronized long size() throws IOException {
        return storage.getBlockSize() * storage.getBlockCount();
    }

    public synchronized void commit() throws IOException {

        storage.beginTransaction();
        for (Long id: dirtyBlocks) {
            storage.putBlock(id, cachedBlocks.get(id));
        }

        storage.endTransaction();
        dirtyBlocks.clear();

    }




}
