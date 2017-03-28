package com.dataheaps.blockstorage;

import org.apache.commons.compress.compressors.FileNameUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by admin on 9/3/17.
 */
public class LocalFileBlockStorage extends PersistentBlockStorage {

    File sourceDir;
    File workingDir;
    boolean inTx = false;

    public LocalFileBlockStorage(int blockSize, File sourceDir, File workingDir) {
        super(blockSize);
        this.sourceDir = sourceDir;
        this.workingDir = workingDir;
    }

    @Override
    public void open() throws IOException {
        commitPendingBlocks();
    }

    @Override
    public void close() throws IOException {
        commitPendingBlocks();
    }

    protected void commitPendingBlocks() throws IOException {
        boolean inTx = Paths.get(sourceDir.getAbsolutePath(), "tx.started").toFile().exists();
        if (inTx) FileUtils.deleteQuietly(Paths.get(sourceDir.getAbsolutePath(), "tx.started").toFile());

        for (File f: sourceDir.listFiles()) {
            if (f.getName().endsWith(".txb")) {
                if (!inTx) {
                    FileUtils.copyFile(
                            f.getAbsoluteFile(),
                            Paths.get(sourceDir.getAbsolutePath(), FilenameUtils.removeExtension(f.getName()) + ".blk").toFile()
                    );
                }
                FileUtils.deleteQuietly(f);
            }
        }
    }

    protected int getBlocksCount() {
        int ctr = 0;
        for (File f: sourceDir.listFiles()) {
            if (f.getName().endsWith(".blk")) {
                ctr++;
            }
        }
        return ctr;
    }

    @Override
    protected File getBlockFile(long id) throws IOException {
        File src = Paths.get(sourceDir.getAbsolutePath(), Long.toString(id) + ".blk").toFile();
        File dest = Paths.get(workingDir.getAbsolutePath(), Long.toString(id) + ".blk").toFile();
        if (src.exists()) {
            FileUtils.copyFile(src, dest);
        }
        else {
            FileUtils.touch(dest);
        }
        return dest;
    }

    @Override
    protected void putBlockFile(long id, File f) throws IOException {
        File dest = Paths.get(sourceDir.getAbsolutePath(), Long.toString(id) + (inTx ? ".txb" : ".blk")).toFile();
        FileUtils.copyFile(Paths.get(workingDir.getAbsolutePath(), Long.toString(id) + ".blk").toFile(), dest);
    }

    @Override
    public synchronized void beginTransaction() throws IOException {
        inTx = true;
        FileUtils.touch(Paths.get(sourceDir.getAbsolutePath(), "tx.started").toFile());
    }

    @Override
    public synchronized void endTransaction() throws IOException {
        FileUtils.deleteQuietly(Paths.get(sourceDir.getAbsolutePath(), "tx.started").toFile());
        commitPendingBlocks();
        inTx = false;
    }
}
