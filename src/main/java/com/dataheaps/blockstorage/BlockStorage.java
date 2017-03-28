package com.dataheaps.blockstorage;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by admin on 20/1/17.
 */
public interface BlockStorage {

    void open() throws IOException;
    void close() throws IOException;
    int getBlockSize() throws IOException;
    long getBlockCount() throws IOException;
    ByteBuffer getBlock(long n) throws IOException;
    void putBlock(long n, ByteBuffer block) throws IOException;
    void beginTransaction() throws IOException;
    void endTransaction() throws IOException;


}
