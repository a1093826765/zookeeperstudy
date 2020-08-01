package com.nov.bigdata.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

//客戶端
public class DistributedClient {

    //服务器IP
    private static final String connectString="172.16.106.128:2181,172.16.106.130:2181,172.16.106.131:2181";
    //超时时间
    private static final int sessionTimeout=20000;

    private static boolean zookeeperNotOline=true;
    //主节点
    private  static  final String parentNode="/idea";

    private ZooKeeper zk=null;

    //注意加volatile（线程共享）
    private volatile List<String> serverList;

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
                    getServerList();
                } catch (Exception e) {
                    //重新更新服务器列表，并且注册监听
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
    //获取服务器信息列表
    public void getServerList() throws Exception{
        //获取服务器子 节点信息，并且对 父节点进行监听
        List<String> childrenList=zk.getChildren(parentNode,true);
        //创建一个局部list来存服务器信息
        List<String> servers = new ArrayList<>();
        for(String child:childrenList){
            //child是子節點的节点名
            byte[] data=zk.getData(parentNode+"/"+child,false,null);
            servers.add(new String (data));
        }
        //赋值给成员变量，提供给各业务线程使用
        serverList=servers;

        //打印服务器列表
        System.out.println(serverList);
    }

    //业务功能
    public void handleBussiness() throws InterruptedException {
        System.out.println("client start working...");
        Thread.sleep(Long.MAX_VALUE);
    }


    public static void main(String[] args) throws Exception {

        //获取zk链接
        DistributedClient client = new DistributedClient();
        client.getConnect();

        //获取servers的子节点信息（并监听），从中获取服务器信息列表
        client.getServerList();

        //业务线程
        client.handleBussiness();
    }
}
