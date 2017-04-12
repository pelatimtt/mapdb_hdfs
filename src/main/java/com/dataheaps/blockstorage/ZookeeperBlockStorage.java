package com.dataheaps.blockstorage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.iq80.snappy.Snappy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by admin on 12/4/17.
 */
public class ZookeeperBlockStorage implements BlockStorage, Watcher {


    static final int BLOCK_SIZE = 1024*256;
    static String BLOCK_KEY = "block_";
    static final String BLOCKS_PATH = "blocks";
    static final String TX_BLOCKS_PATH = "txblocks";
    static final String TX_IN_PROGRESS_PATH = "tx_in_progress";
    static final byte[] TRUE_BYTE = new byte[] {(byte)1};
    static final byte[] FALSE_BYTE = new byte[] {(byte)0};

    String zkQuorum;
    Path zkPath;
    boolean inTx = false;
    Map<Long, ByteBuffer> txBlocks = new HashMap<>();
    CuratorFramework client;

    Map<Long, ByteBuffer> blocksCache = new HashMap<>();

    public ZookeeperBlockStorage(String zkQuorum, Path zkPath) {
        this.zkQuorum = zkQuorum;
        this.zkPath = zkPath;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

    }

    @Override
    public void open() throws IOException {

        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            client = CuratorFrameworkFactory.newClient(zkQuorum, retryPolicy);
            client.start();
            client.blockUntilConnected();

            ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), zkPath.toString().toString());
            ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), Paths.get(zkPath.toString(), BLOCKS_PATH).toString());
            ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), Paths.get(zkPath.toString(), TX_BLOCKS_PATH).toString());
            commitPendingBlocks();

        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    void commitPendingBlocks() throws Exception {
        if (client.checkExists().forPath(Paths.get(zkPath.toString(), TX_IN_PROGRESS_PATH).toString()) == null) {
            client.create().forPath(Paths.get(zkPath.toString(), TX_IN_PROGRESS_PATH).toString(), FALSE_BYTE);
        }
        else {
            byte[] tx = client.getData().forPath(Paths.get(zkPath.toString(), TX_IN_PROGRESS_PATH).toString());
            List<String> children = client.getChildren().forPath(Paths.get(zkPath.toString(), TX_BLOCKS_PATH).toString());
            if (Arrays.equals(tx, TRUE_BYTE)) {
                for (String child: children)
                    client.delete().forPath(Paths.get(zkPath.toString(), TX_BLOCKS_PATH, child).toString());
            }
            else {
                for (String child: children) {
                    byte[] data = client.getData().forPath(Paths.get(zkPath.toString(), TX_BLOCKS_PATH, child).toString());
                    client.setData().forPath(Paths.get(zkPath.toString(), BLOCKS_PATH, child).toString(), data);
                    client.delete().forPath(Paths.get(zkPath.toString(), TX_BLOCKS_PATH, child).toString());
                }
            }
            client.setData().forPath(Paths.get(zkPath.toString(), TX_IN_PROGRESS_PATH).toString(), FALSE_BYTE);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            commitPendingBlocks();
            client.close();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    String resolveKeyName(long id) {
        return BLOCK_KEY + String.format("%08d", id);
    }

    @Override
    public int getBlockSize() throws IOException {
        return BLOCK_SIZE;
    }

    @Override
    public long getBlockCount() throws IOException {
        try {
            return client.getChildren().forPath(zkPath.toString()).size();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public synchronized ByteBuffer getBlock(long n) throws IOException {

        if (blocksCache.containsKey(n))
            return blocksCache.get(n);

        try {

            Path src = Paths.get(zkPath.toString(), BLOCKS_PATH, resolveKeyName(n));
            ByteBuffer bytes = null;
            if (client.checkExists().forPath(src.toString()) != null) {
                byte[] raw = client.getData().forPath(src.toString());
                bytes = ByteBuffer.wrap(raw);
            } else {
                client.create().forPath(src.toString(), new byte[BLOCK_SIZE]);
                bytes = ByteBuffer.allocate(BLOCK_SIZE);
            }
            blocksCache.put(n, bytes);
            return bytes;
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }


    @Override
    public void putBlock(long n, ByteBuffer block) throws IOException {

        try {
            if (inTx) {
                Path dst = Paths.get(zkPath.toString(), TX_BLOCKS_PATH, resolveKeyName(n));
                if (client.checkExists().forPath(dst.toString()) == null)
                    client.create().forPath(dst.toString(), block.array());
                else
                    client.setData().forPath(dst.toString(), block.array());
                txBlocks.put(n, block);
                client.sync();
            }
            else {
                Path dst = Paths.get(zkPath.toString(), BLOCKS_PATH, resolveKeyName(n));
                client.setData().forPath(dst.toString(), block.array());
            }
        }
        catch (Exception e) {
            throw new IOException(e);
        }

    }

    @Override
    public synchronized void beginTransaction() throws IOException {
        try {
            client.setData().forPath(Paths.get(zkPath.toString(), TX_IN_PROGRESS_PATH).toString(), TRUE_BYTE);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
        finally {
            inTx = true;
        }
    }

    @Override
    public synchronized void endTransaction() throws IOException {
        try {
            client.setData().forPath(Paths.get(zkPath.toString(), TX_IN_PROGRESS_PATH).toString(), FALSE_BYTE);
            for (Map.Entry<Long, ByteBuffer> e: txBlocks.entrySet()) {
                Path src = Paths.get(zkPath.toString(), TX_BLOCKS_PATH, resolveKeyName(e.getKey()));
                Path dst = Paths.get(zkPath.toString(), BLOCKS_PATH, resolveKeyName(e.getKey()));
                client.setData().forPath(dst.toString(), e.getValue().array());
                client.delete().forPath(src.toString());
            }
            client.sync();
            txBlocks.clear();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
        finally {
            inTx = false;
        }
    }

}
