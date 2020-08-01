package com.nov.bigdata.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.lang.Thread.sleep;


public class SimpleZkClient {
    //服务器IP
    private static final String connectString="172.16.106.128:2181,172.16.106.130:2181,172.16.106.131:2181";
    //超时时间
    private static final int sessionTimeout=20000;

    private static boolean zookeeperNotOline=true;
    ZooKeeper zkClient=null;

    @Before
    public void init()throws Exception{
        //由于连接zk需要时间，所以这里使用countDownLatch
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        zkClient=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //收到通知后的回调函数（事件处理逻辑）
                System.out.println(watchedEvent.getType()+"----"+watchedEvent.getPath());
                try {
                    //对根目录进行监听
                    zkClient.getChildren("/",true);
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
        if (ZooKeeper.States.CONNECTING.equals(zkClient.getState())){
            System.out.println("连接中");
            countDownLatch.await();
        }
    }

    /**
     * 数据的增删改查
     */

    //创建新的节点
    @Test
    public void teestCreate() throws Exception{
        //参数1：创建节点的路径  参数2：节点的数据 参数3：节点的权限  参数4：节点的类型  返回：创建节点的路径
        //上传数据可以是任何类型  但都要转成bytes
        String nodeCreated=zkClient.create("/idea","helloZk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //获取子节点
    @Test
    public  void getChildren() throws Exception{
        List<String> children=zkClient.getChildren("/",true);
        for(String child:children){
            System.out.println(child);
        }
        sleep(Long.MAX_VALUE);
    }

    //判断znode是否存在
    @Test
    public void testExist() throws Exception{
        Stat stat = zkClient.exists("/idea", false);
        System.out.println(stat==null?"not exist":"exist");
    }

    //获取znode的数据
    @Test
    public void getData() throws  Exception{
        byte[] data=zkClient.getData("/idea",false,new Stat());
        System.out.println(new String(data));
    }

    //删除znode
    @Test
    public void deletdZode() throws Exception{
        //参数2：指删除的版本，-1表示删除所有版本
        zkClient.delete("/idea",-1);
    }

    //修改Data
    @Test
    public void setData() throws Exception{
        zkClient.setData("/idea","hi".getBytes(),-1);
    }

}
