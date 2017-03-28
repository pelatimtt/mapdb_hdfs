package com.dataheaps.blockstorage;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

/**
 * Created by admin on 9/3/17.
 */

@RunWith(Parameterized.class)
public class BlockMemoryTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { 5000 }, { 50000 }
        });
    }

    int blockSize;

    public BlockMemoryTest(int blockSize) {
        this.blockSize = blockSize;
    }

    @org.junit.Test
    public void testBlockStorage() throws Exception {

        File src = Files.createTempDir();

        BlockStorage bs = new LocalFileBlockStorage(blockSize, src, Files.createTempDir());
        BlockMemory bm = new BlockMemory(bs);
        bm.open();

        File f = new File("src/test/resources/test.jpg");

        FileInputStream is = new FileInputStream(f);
        byte[] v = new byte[600];
        long written = 0;
        while (is.available() > 0) {
            int read = is.read(v);
            //bm.write(written, v, read);
            bm.write(written, ByteBuffer.wrap(v, 0, read));
            written += read;
        }

        bm.close();

        bs = new LocalFileBlockStorage(blockSize, src, Files.createTempDir());
        bm = new BlockMemory(bs);
        bm.open();

        File tmp = File.createTempFile("testblockstorage", "jpg");
        FileOutputStream os = new FileOutputStream(tmp);
        //byte[] readed = new byte[(int)f.length()];
        //bm.read(0, readed, readed.length);

        ByteBuffer buf = ByteBuffer.allocate((int)f.length());
        bm.read(0, buf);

        os.write(buf.array());
        os.flush();
        os.close();

        bm.close();

        assert(FileUtils.contentEquals(f, tmp));

    }

}