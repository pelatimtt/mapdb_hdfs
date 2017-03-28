package com.dataheaps.blockstorage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by admin on 27/3/17.
 */
public class HdfsBlockStorage extends PersistentBlockStorage {

    String sourceDir;
    File workingDir;
    FileSystem fs;
    boolean inTx = false;

    public HdfsBlockStorage(int blockSize, FileSystem fs, String sourceDir, File workingDir) {
        super(blockSize);
        this.fs = fs;
        this.sourceDir = sourceDir;
        this.workingDir = workingDir;
    }

    @Override
    public void open() throws IOException {
        if (!fs.exists(new Path(sourceDir)))
            fs.mkdirs(new Path(sourceDir));
        commitPendingBlocks();
    }

    @Override
    public void close() throws IOException {
        commitPendingBlocks();
    }

    protected void commitPendingBlocks() throws IOException {

        boolean inTx = fs.exists(new Path(sourceDir, "tx.started"));
        if (inTx) fs.delete(new Path(sourceDir, "tx.started"), false);

        RemoteIterator<LocatedFileStatus> i = fs.listFiles(new Path(sourceDir), false);
        while (i.hasNext()) {
            LocatedFileStatus f = i.next();
            if (f.getPath().getName().endsWith(".txb")) {
                if (!inTx) {
                    fs.delete(new Path(sourceDir, FilenameUtils.removeExtension(f.getPath().getName()) + ".blk"), false);
                    fs.rename(
                           f.getPath(), new Path(sourceDir, FilenameUtils.removeExtension(f.getPath().getName()) + ".blk")
                    );
                }
                fs.delete(f.getPath(), false);
            }
        }
    }

    protected int getBlocksCount() throws IOException {
        int ctr = 0;
        RemoteIterator<LocatedFileStatus> i = fs.listFiles(new Path(sourceDir), false);
        while (i.hasNext()) {
            LocatedFileStatus f = i.next();
            if (f.getPath().getName().endsWith(".blk")) {
                ctr++;
            }
        }
        return ctr;
    }

    @Override
    protected File getBlockFile(long id) throws IOException {
        Path src = new Path(sourceDir, Long.toString(id) + ".blk");
        File dest = Paths.get(workingDir.getAbsolutePath(), Long.toString(id) + ".blk").toFile();
        if (fs.exists(src)) {
            fs.copyToLocalFile(src, new Path(dest.getAbsolutePath()));
            FileUtils.deleteQuietly(new File(workingDir.getAbsolutePath(), "." + Long.toString(id) + ".blk.crc"));
        }
        else {
            FileUtils.touch(dest);
        }
        return dest;
    }

    @Override
    protected void putBlockFile(long id, File f) throws IOException {
        Path dest = new Path(sourceDir, Long.toString(id) + (inTx ? ".txb" : ".blk"));
        fs.copyFromLocalFile(new Path(workingDir.getAbsolutePath(), Long.toString(id) + ".blk"), dest);
    }

    @Override
    public synchronized void beginTransaction() throws IOException {
        inTx = true;
        fs.createNewFile(new Path(sourceDir, "tx.started"));
    }

    @Override
    public synchronized void endTransaction() throws IOException {
        fs.delete(new Path(sourceDir, "tx.started"), false);
        commitPendingBlocks();
        inTx = false;
    }
}
