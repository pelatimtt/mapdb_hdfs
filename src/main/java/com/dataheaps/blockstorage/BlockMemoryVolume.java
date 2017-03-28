package com.dataheaps.blockstorage;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.mapdb.CC;
import org.mapdb.DataInput2;
import org.mapdb.volume.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 21/1/17.
 */


public class BlockMemoryVolume extends Volume {

    Logger logger = LoggerFactory.getLogger(BlockMemoryVolume.class);


    BlockMemory memory;

    public BlockMemoryVolume(BlockMemory memory) {
        this.memory = memory;
    }

    public void ensureAvailable(long l) {
        System.out.print("");
    }

    public void truncate(long l) {
        System.out.print("");
    }

    public void putLong(long l, long l1) {
        try {
            ByteBuffer b = ByteBuffer.allocate(8);
            b.putLong(l1);
            b.position(0);
            memory.write(l, b);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void putInt(long l, int i) {
        try {
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(i);
            b.position(0);
            memory.write(l, b);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void putByte(long l, byte b) {
        try {
            memory.write(l, new byte[]{b}, 1);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void putData(long l, byte[] src, int srcPos, int srcSize) {

        ByteBuffer buf = ByteBuffer.wrap(src,srcPos, srcSize);
        buf.position(0);
        putData(l, buf);
    }

    public void putData(long l, ByteBuffer byteBuffer) {
        try {
            memory.write(l, byteBuffer);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getLong(long offset) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(8);
            memory.read(offset, buf);
            return buf.getLong(0);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getInt(long offset) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(4);
            memory.read(offset, buf);
            return buf.getInt(0);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte getByte(long offset) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(1);
            memory.read(offset, buf);
            return buf.get(0);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataInput2.ByteBuffer getDataInput(long offset, int size) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(size);
            memory.read(offset, buf);
            buf.position(0);
            return new DataInput2.ByteBuffer(buf, 0);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getData(long offset, byte[] bytes, int bytesPos, int size) {

        try {
            ByteBuffer buf = ByteBuffer.wrap(bytes, bytesPos, size);
            memory.read(offset, buf);
            buf.position(0);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void close() {
        try {
            memory.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void sync() {
        try {
            memory.commit();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int sliceSize() {
        return 64000;
    }

    public boolean isSliced() {
        return true;
    }

    public long length() {
        try {
            return memory.size();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isReadOnly() {
        return false;
    }

    public File getFile() {
        return null;
    }

    public boolean getFileLocked() {
        return false;
    }

    public void clear(long l, long l1) {
        byte[] buf = new byte[(int)(l1-l)];
        try {
   //         memory.clear(l, (int)(l1-l));
            memory.write(l, buf, buf.length);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
