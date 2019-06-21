package com.demo.distributed.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * 分布式进程消息共享消息
 */
public class DistributeCache {
    private static List<String> msgCache = new ArrayList<String>();
    static class MsgConsumer extends Thread{
        @Override
        public void run(){
            if (!msgCache.isEmpty()){
                for (int i = 0;i < msgCache.size();i++){
                    String lock = ZookeeperLock.getInstance().getLock();
                    if (msgCache.isEmpty()){
                        return;
                    }
                    String msg = msgCache.get(0);
                    System.out.println(Thread.currentThread().getName()+" consume msg:" + msg);
                    try {
                        Thread.sleep(1000);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    msgCache.remove(msg);
                    ZookeeperLock.getInstance().releaseLock(lock);
                }
            }

        }
    }

    public static void main(String[] args){
        for(int i = 0;i < 10;i++){
            msgCache.add("msg"+i);
        }
        MsgConsumer consumer1 = new MsgConsumer();
        MsgConsumer consumer2 = new MsgConsumer();
        consumer1.start();
        consumer2.start();
        try {
            consumer1.join();
            consumer2.join();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println("complete,now delete lock path:");
        ZookeeperLock.getInstance().releaseLock("/Locks");

    }

}
