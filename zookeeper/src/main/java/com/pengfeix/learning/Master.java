package com.pengfeix.learning;

import java.util.Random;
import java.util.logging.Logger;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;


public class Master implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    ZooKeeper zk;
    String hostPort;
    Boolean isLeader = false;

    private Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());

    StringCallback masterCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") + "the leader");
        }
    };

    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    /*
    Boolean checkMaster() throws KeeperException, InterruptedException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (NoNodeException e) {
                return false;
            } catch (ConnectionLossException e) {
            }
        }
    }
    */

    DataCallback masterCheckCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
                    return;
            }
        }
    };

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    void runForMaster() {
        zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    /*
    void runForMaster() throws KeeperException, InterruptedException {
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (NodeExistsException e) {
                isLeader = false;
                break;
            } catch (ConnectionLossException e) {
            }

            if (checkMaster()) break;
        }
    }
    */

    void startZk() throws java.io.IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZk() throws Exception {
        zk.close();
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path,
                  data,
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT,
                  createParentCallback,
                  data);
    }

    StringCallback createParentCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    LOG.info("Parent created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered: " + path);
                    break;
                default:
                    LOG.error("Something went wrong: ", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    public static void main(String[] args) throws Exception {
        Master m = new Master(args[0]);
        m.startZk();

        m.runForMaster();

        Thread.sleep(60000);

        if (m.isLeader) {
            System.out.println("i am the Leader");
            Thread.sleep(6000);
        } else {
            System.out.println("someone else is the Leader");
        }

        m.bootstrap();
        Thread.sleep(60000);

        m.stopZk();
    }
}
