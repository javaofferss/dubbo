/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.zookeeper.curator5;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that exercises the curator2 drop-in {@link Curator5ZookeeperClient} against a
 * real, in-process Apache ZooKeeper 3.4.5 server.
 *
 * <p>The server is started in-process from the {@code org.apache.zookeeper:zookeeper:3.4.5} jar that
 * this module depends on (see {@code zookeeper34.version} in the pom), via {@link ZooKeeperServer}
 * + {@link NIOServerCnxnFactory}. This is the genuine ZK 3.4.5 server implementation — the same
 * one a standalone {@code zkServer.sh} of the 3.4.5 distribution would run — so it proves the
 * curator2 client speaks the ZK 3.4.x wire protocol correctly.
 */
class Curator5ZookeeperClientRealZkTest {

    private static NIOServerCnxnFactory cnxnFactory;
    private static ZooKeeperServer zkServer;
    private static File dataDir;
    private static File dataLogDir;
    private static int port;
    private static String zkAddress;

    private Curator5ZookeeperClient client;
    private String root;

    @BeforeAll
    static void startZk345() throws Exception {
        port = freePort();
        zkAddress = "127.0.0.1:" + port;

        dataDir = Files.createTempDirectory("curator2-test-data").toFile();
        dataLogDir = Files.createTempDirectory("curator2-test-datalog").toFile();

        zkServer = new ZooKeeperServer(dataDir, dataLogDir, 3000);
        cnxnFactory = new NIOServerCnxnFactory();
        cnxnFactory.configure(new InetSocketAddress("127.0.0.1", port), 100);
        cnxnFactory.startup(zkServer);

        // wait until the server accepts connections
        await().untilAsserted(() -> {
            try (java.net.Socket socket = new java.net.Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 1000);
                assertTrue(socket.isConnected());
            }
        });
    }

    @AfterAll
    static void stopZk345() {
        try {
            if (cnxnFactory != null) {
                cnxnFactory.shutdown();
            }
        } catch (Exception ignored) {
        }
        try {
            if (zkServer != null) {
                zkServer.shutdown();
            }
        } catch (Exception ignored) {
        }
        if (dataDir != null) {
            deleteRecursively(dataDir);
        }
        if (dataLogDir != null) {
            deleteRecursively(dataLogDir);
        }
    }

    @BeforeEach
    void setUp() {
        URL url = URL.valueOf("zookeeper://" + zkAddress + "?check=true");
        client = new Curator5ZookeeperClient(url);
        assertTrue(client.isConnected(), "curator2 client should connect to in-process ZK 3.4.5");

        root = "/curator2-realzk-test-" + UUID.randomUUID().toString().replace('-', '_');
        client.create(root, false, true); // persistent root (parent "/" always exists)
    }

    @AfterEach
    void tearDown() {
        if (client == null) {
            return;
        }
        try {
            client.delete(root); // deletingChildrenIfNeeded removes the whole subtree
        } catch (Exception ignored) {
        } finally {
            client.close();
        }
    }

    @Test
    @Timeout(10)
    void createPersistentAndCheckExists() {
        String path = root + "/persistent";
        client.create(path, false, true);
        assertTrue(client.checkExists(path));
        // Note: curator 2.x's CreateBuilder.forPath(String) (no explicit data) writes the framework's
        // default data, which CuratorFrameworkFactory initialises to InetAddress.getLocalHost() bytes —
        // a machine-dependent value. So getContent() here is non-null (the local host address), unlike
        // curator 5.x which writes empty data. This is benign for Dubbo: no-data createPersistent is only
        // used for structural parent nodes whose content is never read. Data-bearing nodes use createOrUpdate.
        assertNotNull(client.getContent(path), "curator 2.x no-data node carries the framework default (localhost) data");
    }

    @Test
    @Timeout(10)
    void createEphemeralWithCheckExists() {
        String path = root + "/ephemeral";
        client.create(path, true, true);
        assertTrue(client.checkExists(path));
    }

    @Test
    @Timeout(10)
    void createOrUpdateChangesContent() {
        String path = root + "/updatable";
        client.createOrUpdate(path, "v1", false);
        assertEquals("v1", client.getContent(path));

        client.createOrUpdate(path, "v2", false);
        assertEquals("v2", client.getContent(path));
    }

    @Test
    @Timeout(10)
    void getChildren() {
        client.create(root + "/c1", false, true);
        client.create(root + "/c2", false, true);
        List<String> children = client.getChildren(root);
        assertNotNull(children);
        assertTrue(children.contains("c1"));
        assertTrue(children.contains("c2"));
    }

    @Test
    @Timeout(10)
    void getConfigItemReturnsStat() {
        String path = root + "/configitem";
        client.createOrUpdate(path, "data", false);
        ConfigItem item = client.getConfigItem(path);
        assertNotNull(item);
        assertEquals("data", item.getContent());
        assertNotNull(item.getTicket(), "ConfigItem should carry a non-null stat/ticket");
        org.apache.zookeeper.data.Stat stat = (org.apache.zookeeper.data.Stat) item.getTicket();
        assertTrue(stat.getVersion() >= 0);
    }

    @Test
    @Timeout(15)
    void childListenerFiresOnChildAdded() {
        CopyOnWriteArrayList<List<String>> seen = new CopyOnWriteArrayList<>();
        ChildListener listener = (path, children) -> seen.add(children);
        try {
            List<String> initial = client.addChildListener(root, listener);
            assertNotNull(initial);

            client.create(root + "/watched-child", false, true);

            await().untilAsserted(() -> {
                assertFalse(seen.isEmpty(), "childChanged should have fired");
                List<String> last = seen.get(seen.size() - 1);
                assertNotNull(last);
                assertTrue(last.contains("watched-child"), "last childChanged should list the new child");
            });
        } finally {
            client.removeChildListener(root, listener);
        }
    }

    @Test
    @Timeout(15)
    void dataListenerFiresOnDataChange() {
        String path = root + "/datawatch";
        client.createOrUpdate(path, "v1", false);

        AtomicReference<String> latest = new AtomicReference<>();
        AtomicReference<EventType> latestType = new AtomicReference<>();
        DataListener listener = (p, value, type) -> {
            if (value instanceof String) {
                latest.set((String) value);
            }
            latestType.set(type);
        };
        try {
            client.addDataListener(path, listener);

            client.createOrUpdate(path, "v2", false);

            String finalContent = "v2";
            await().untilAsserted(() -> {
                assertEquals(finalContent, latest.get(), "dataListener should see updated content");
                assertEquals(EventType.NodeDataChanged, latestType.get(), "event type should be NodeDataChanged");
            });
        } finally {
            client.removeDataListener(path, listener);
        }
    }

    @Test
    @Timeout(10)
    void deleteRemovesNode() {
        String path = root + "/todelete";
        client.create(path, false, true);
        assertTrue(client.checkExists(path));
        client.delete(path);
        assertFalse(client.checkExists(path));
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void deleteRecursively(File f) {
        if (f == null || !f.exists()) {
            return;
        }
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File c : children) {
                    deleteRecursively(c);
                }
            }
        }
        f.delete();
    }
}
