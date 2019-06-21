package com.demo.distributed.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Krystal on 2019/6/20.
 */
public class ZKCurator {
    CuratorFramework curatorFramework = null;
    @Before
    public void connect(){
        String kvm = "192.168.48.129:2181";
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(kvm).sessionTimeoutMs(4000).retryPolicy(new ExponentialBackoffRetry(100,3))
                .namespace("curator").build();
        curatorFramework.start();
    }

    @Test
    public void create(){
        try{
            curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                    .forPath("/krystal/node1","1".getBytes());
            Stat stat = new Stat();
            curatorFramework.getData().storingStatIn(stat).forPath("/krystal/node1");
            curatorFramework.setData().withVersion(stat.getVersion()).forPath("/krystal/node1","2".getBytes());
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @After
    public void close(){
       curatorFramework.close();
    }

}
