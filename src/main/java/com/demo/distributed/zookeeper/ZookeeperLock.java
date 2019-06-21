package com.demo.distributed.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created by Krystal on 2019/6/20.
 * 分布式锁
 */
public final class ZookeeperLock {
    private static ZooKeeper zkClient = null;
    private static String ROOT_LOCK_PATH = "/Locks";
    private String PRE_LOCK_NAME = "mylock_";

    private static class ZookeeperLockHold{
        private static ZookeeperLock lock = new ZookeeperLock();
        static {
            initZkClient();
        }

    }
    public static ZookeeperLock getInstance() {
        return ZookeeperLockHold.lock;
    }

    private static void initZkClient() {
        String zookeeperIp = "192.168.48.129:2181";
        if (null == zkClient) {
            try {
                zkClient = new ZooKeeper(zookeeperIp, 3000, null);
                // 判断节点是否存在
                Stat stat = zkClient.exists(ROOT_LOCK_PATH, false);
                if(null == stat){
                    // 创建根节点
                    zkClient.create(ROOT_LOCK_PATH ,"".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }

            } catch (IOException  | KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取锁：实际上是创建线程目录，并判断线程目录序号是否最小
     *
     * @return
     */
    public String getLock() {
        try {
            String lockPath = zkClient.create(ROOT_LOCK_PATH + '/' + PRE_LOCK_NAME,
                    Thread.currentThread().getName().getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + " create lock path : " + lockPath);
            tryLock(lockPath);
            return lockPath;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean tryLock(String lockPath) throws KeeperException, InterruptedException {
        List<String> lockPaths = zkClient.getChildren(ROOT_LOCK_PATH, false);
        Collections.sort(lockPaths);
        int index = lockPaths.indexOf(lockPath.substring(ROOT_LOCK_PATH.length() + 1));
        if (index == 0) {
            System.out.println(Thread.currentThread().getName() + " get lock,lock path:" + lockPath);
            return true;
        } else {
            // 创建watcher ，监控lockPath的前一个节点
            Watcher watcher = new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println("Received delete event, node path is " + event.getPath());
                    synchronized (this) {
                        notifyAll();
                    }
                }
            };
            String preLockPath = lockPaths.get(index - 1);
            // 查询前一个目录是否存在，并且注册目录事件监听器，监听一次事件后即删除
            Stat state = zkClient.exists(ROOT_LOCK_PATH + '/' + preLockPath, watcher);
            if (state == null) {
                return tryLock(lockPath);
            } else {
                System.out.println(Thread.currentThread().getName() + " wait for " + preLockPath);
                synchronized (watcher) {
                    watcher.wait();
                }
                return tryLock(lockPath);
            }
        }
    }

    /**
     * 释放锁
     *
     * @param lockPath
     */
    public void releaseLock(String lockPath) {
        try {
            if (lockPath.equals(ROOT_LOCK_PATH)){
                List<String> lockPaths = zkClient.getChildren(ROOT_LOCK_PATH, false);
                if (null != lockPaths && !lockPath.isEmpty()){
                    for (String path:lockPaths){
                        System.out.println(path);
                        zkClient.delete(ROOT_LOCK_PATH+'/'+path, -1);
                    }
                }
            }
            zkClient.delete(lockPath, -1);
            System.out.println("Release lock,lock path is " + lockPath);

        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

}
