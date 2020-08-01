package com.nov.bigdata.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

//服務端
public class DistributedServer {
    //服务器IP
    private static final String connectString="172.16.106.128:2181,172.16.106.130:2181,172.16.106.131:2181";
    //超时时间
    private static final int sessionTimeout=20000;

    private static boolean zookeeperNotOline=true;
    //主节点
    private  static  final String parentNode="/idea";
    private ZooKeeper zk=null;

    //创建到zk的客户端链接
    public void getConnect() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        zk=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //收到通知后的回调函数（事件处理逻辑）
                System.out.println(watchedEvent.getType()+"----"+watchedEvent.getPath());
                try {
                    //对根目录进行监听
                    zk.getChildren("/",true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if(zookeeperNotOline) {
                    countDownLatch.countDown();
                    zookeeperNotOline=false;
                    System.out.println("连接完成");
                }
            }
        });
        if (ZooKeeper.States.CONNECTING.equals(zk.getState())){
            System.out.println("连接中");
            countDownLatch.await();
        }
    }

    //向zk集群注册服务器信息
    public  void registerServer(String hostname) throws Exception {
        String nodeCreated=zk.create(parentNode+"/test",hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+"  is  online.."+nodeCreated);
    }

    //业务功能
    public void handleBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname+" start working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        String hostname="idea";
        //获取zk连接
            DistributedServer server=new DistributedServer();
            server.getConnect();
        //利用zk连接注册服务器信息
        server.registerServer(hostname);
        //启动业务功能
        server.handleBussiness(hostname);
        System.out.println(123);
    }
}
