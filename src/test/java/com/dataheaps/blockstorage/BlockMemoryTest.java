package com.dataheaps.blockstorage;

import com.dataheaps.blockstorage.factories.LocalFileStorageFactory;
import com.dataheaps.blockstorage.factories.StorageFactory;
import com.dataheaps.blockstorage.factories.ZookeeperStorageFactory;
import org.apache.commons.io.FileUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

/**
 * Created by admin on 9/3/17.
 */

@RunWith(Parameterized.class)
public class BlockMemoryTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { new LocalFileStorageFactory() },
                { new ZookeeperStorageFactory() }
        });
    }

    StorageFactory f;

    public BlockMemoryTest(StorageFactory f) throws Exception {
        this.f = f;
        f.init();
    }

    @org.junit.Test
    public void testBlockStorage() throws Exception {

        BlockMemory bm = new BlockMemory(this.f.create());
        bm.open();

        File f = new File("src/test/resources/test.jpg");

        FileInputStream is = new FileInputStream(f);
        byte[] v = new byte[600];
        long written = 0;
        while (is.available() > 0) {
            int read = is.read(v);
            bm.write(written, ByteBuffer.wrap(v, 0, read));
            written += read;
        }

        bm.close();

        bm = new BlockMemory(this.f.create());
        bm.open();

        File tmp = File.createTempFile("testblockstorage", "jpg");
        FileOutputStream os = new FileOutputStream(tmp);

        ByteBuffer buf = ByteBuffer.allocate((int)f.length());
        bm.read(0, buf);

        os.write(buf.array());
        os.flush();
        os.close();

        bm.close();

        assert(FileUtils.contentEquals(f, tmp));

    }

}