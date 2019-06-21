package com.demo.distributed.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static com.sun.xml.internal.ws.dump.LoggingDumpTube.Position.Before;

/**
 * Created by Krystal on 2019/6/20.
 */
public class ZKTest {
    private ZooKeeper zooKeeper = null;

    @org.junit.Before
    public void connect() {
        String kvm = "192.168.48.129:2181";
        try {
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            zooKeeper = new ZooKeeper(kvm, 4000, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    // SyncConnected : 同步连接
                    if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                        // 如果收到服务端的响应时间，连接成功
                        countDownLatch.countDown();
                        System.out.println("建立连接成功");
                    }
                }
            });
            countDownLatch.await();
            System.out.println(zooKeeper.getState());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void operate() {
        if (null != zooKeeper && zooKeeper.getState().isConnected()) {
            try {
                // 新增节点
                zooKeeper.create("/zk-krystal", "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                Thread.sleep(1000);
                Stat stat = new Stat();// 状态
                // 2 查看新增后的节点状态
                byte[] bytes = zooKeeper.getData("/zk-krystal", null, stat);

                System.out.println("新增的节点:" + new String(bytes));

                // 3. 修改节点
                zooKeeper.setData("/zk-krystal", "1".getBytes(), stat.getVersion());

                // 4、查看修改后的节点状态
                byte[] byte2 = zooKeeper.getData("/zk-krystal", null, stat);
                System.out.println("修改后的节点:" + new String(byte2));

                // 5、删除节点
                zooKeeper.delete("/zk-krystal", stat.getVersion());
                System.out.println("删除成功.");

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @Ignore
    public void delete() {
        if (null != zooKeeper && zooKeeper.getState().isConnected()) {
            Stat stat = new Stat();// 状态
            try {
                zooKeeper.getData("/zk-test-krystal", null, stat);
                zooKeeper.delete("/zk-test-krystal", stat.getVersion());

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    @Test
    public void watch() {
        if (null != zooKeeper && zooKeeper.getState().isAlive()) {
            try {

                zooKeeper.create("/zk-test-krystal", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                Stat stat = zooKeeper.exists("/zk-test-krystal", new Watcher() {
                    public void process(WatchedEvent watchedEvent) {
                        System.out.println(watchedEvent.getType() + "-->" + watchedEvent.getPath());
                        // 因为watcher是一次性操作，只能看到setData操作带来的变化，
                        // delete操作看不到变化
                        try {
                            zooKeeper.exists(watchedEvent.getPath(), true);
                            System.out.println("节点改变了。");
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
                stat = zooKeeper.setData("/zk-test-krystal", "2".getBytes(), stat.getVersion());
                Thread.sleep(1000);
                // delete
                zooKeeper.delete("/zk-test-krystal", stat.getVersion());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    @After
    public void close() {
        if (null != zooKeeper && zooKeeper.getState().isConnected()) {
            try {
                zooKeeper.close();
                System.out.println("关闭连接成功");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
