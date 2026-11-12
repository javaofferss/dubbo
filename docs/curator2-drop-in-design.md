# Curator2 Drop-in Zookeeper Client 设计文档

> 目标：让 Dubbo 3.3（`3.3.7-xiaomi-SNAPSHOT` 分支）能在 **ZooKeeper 3.4.5** 服务端环境下运行。
> 约束：**只能用 Apache Curator 2.x**（Curator 4.x 已被验证不可用；Curator 5.x 不支持 ZK 3.4）。
> 原则：**不改动现有 curator5 代码**，纯新增模块。

---

## 1. 背景与问题

Dubbo 3.3 默认的 zookeeper 客户端实现是 `dubbo-remoting-zookeeper-curator5`，依赖：

- `org.apache.curator:curator-*` = **5.9.0**
- `org.apache.zookeeper:zookeeper` = **3.7.2**

Curator 5.x **放弃了对 ZooKeeper 3.4 的支持**（Curator 5 要求 ZK 3.5+）。小米内部有 ZK 3.4.5 服务端集群，curator5 客户端连上去会有兼容问题。

经过验证，**Curator 4.x 在该环境也不可用**，只能用 **Curator 2.x**（Curator 2 是 ZK 3.4 时代的 curator，原生支持 ZK 3.4）。

---

## 2. 现有架构（为什么不能简单换版本）

### 2.1 curator5 模块的结构

`dubbo-remoting/dubbo-remoting-zookeeper-curator5` 模块自己定义了一套 zookeeper 客户端抽象，**不是**通过经典 Dubbo SPI 注册的：

```
org.apache.dubbo.remoting.zookeeper.curator5/
  ZookeeperClient            (接口)
  AbstractZookeeperClient    (抽象基类)
  ZookeeperClientManager     (每 ApplicationModel 一个单例，持 client Map)
  Curator5ZookeeperClient    (curator5 实现)
  ChildListener / DataListener / StateListener / EventType
  aot/Curator5ZookeeperReflectionTypeDescriberRegistrar  (AOT 反射注册)
```

注意：`ZookeeperClient` 接口和 `ZookeeperClientManager` 都定义在 **`curator5` 包内部**，模块没有 `META-INF/dubbo/org.apache.dubbo.remoting.zookeeper.ZookeeperClient` 之类的 SPI 文件。

### 2.2 三个消费模块硬编依赖 curator5

下列三个模块在 **编译期直接 `import org.apache.dubbo.remoting.zookeeper.curator5.*`**，并直接依赖 `dubbo-remoting-zookeeper-curator5`：

| 模块 | 直接 import curator5 的文件 |
|------|------|
| `dubbo-registry-zookeeper` | `ZookeeperRegistry.java`、`ZookeeperRegistryFactory.java`（import `curator5.{ZookeeperClient, ZookeeperClientManager, ChildListener, StateListener}`，调 `ZookeeperClientManager.getInstance(model)`） |
| `dubbo-metadata-report-zookeeper` | 2 个文件 |
| `dubbo-configcenter-zookeeper` | 3 个文件 |

**结论**：curator5 的客户端类型是「编译期硬编」进消费模块的，没有抽象层 / SPI 可以在运行期换实现。所以：

- **全局把 curator 5.9.0 换成 2.12.0** —— 行不通：`Curator5ZookeeperClient` 里有一处 `CuratorFrameworkFactory.Builder.ensembleTracker(boolean)` 直调，这个方法是 **curator 5.x 独有**，curator 2.x 没有，编译不过（除非改 curator5 源码改成反射，但这违背「不动现有代码」）。
- **抽 SPI 抽象层** —— 要改 curator5 模块 + 三个消费模块（移接口、改 import、换依赖），改动面大且动现有代码。

---

## 3. 方案：Drop-in 替换 jar（同包名同类名）

核心思路：**新建一个 curator2 实现模块，包名和类名与 curator5 完全一致**，内部用 curator 2.x 实现。下游把 `dubbo-remoting-zookeeper-curator5` 这个 jar **排除掉、换成 curator2 jar**，三个消费模块的 `import org.apache.dubbo.remoting.zookeeper.curator5.*` 自动落到 curator2 的实现上——**消费模块源码一行不改**。

### 3.1 为什么这样可行

Java 的 `import` 只是在编译期把「包名.类名」解析成符号引用，运行期类加载时由 **classpath 上实际存在的 jar** 决定这个类是哪个实现。`dubbo-remoting-zookeeper-curator5` 和 `dubbo-remoting-zookeeper-curator2` 两个 jar 里都有 `org.apache.dubbo.remoting.zookeeper.curator5.Curator5ZookeeperClient` 这个全限定类名，**二选一放 classpath**，消费模块都能跑。

### 3.2 为什么「同包名同类名」不算脏

命名上 curator2 的代码住在一个叫 `curator5` 的包里、类名还叫 `Curator5ZookeeperClient`，确实不好看。但这是 drop-in 替换的必要代价：

- 不这样就得 fork 三个消费模块（改 import 包名），重复维护两套 registry/metadata/configcenter。
- drop-in 的本质就是「同一个全限定名、不同实现」，类名/包名必须一致。

---

## 4. 实现

### 4.1 新增模块 1：`dubbo-remoting-zookeeper-curator2`

位置：`dubbo-remoting/dubbo-remoting-zookeeper-curator2/`

**src 树**：从 curator5 模块原样拷贝（同包名同类名）：

```
src/main/java/org/apache/dubbo/remoting/zookeeper/curator5/
  ZookeeperClient.java                  (原样)
  AbstractZookeeperClient.java          (原样)
  ZookeeperClientManager.java           (原样 — 引用同名 Curator5ZookeeperClient)
  ChildListener / DataListener / StateListener / EventType   (原样)
  Curator5ZookeeperClient.java          (改：去掉 ensembleTracker)
  aot/Curator5ZookeeperReflectionTypeDescriberRegistrar.java (改：去掉 OldServiceInstance)
src/main/resources/META-INF/dubbo/internal/
  org.apache.dubbo.aot.api.ReflectionTypeDescriberRegistrar  (原样：curator5Zookeeper=...)
```

**与 curator5 的差异（仅两处）**：

1. **`Curator5ZookeeperClient.java` 去掉 `ensembleTracker`**

   curator5 原码（构造器里的 builder 链）：
   ```java
   boolean ensembleTracker = url.getParameter(ZOOKEEPER_ENSEMBLE_TRACKER_KEY, DEFAULT_ENSEMBLE_TRACKER);
   CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
           .connectString(url.getBackupAddress())
           .retryPolicy(new RetryNTimes(1, 1000))
           .connectionTimeoutMs(timeout)
           .ensembleTracker(ensembleTracker)   // ← curator 5.x 独有方法
           .sessionTimeoutMs(sessionExpireMs);
   ```
   curator2 版本删掉 `.ensembleTracker(...)` 那一行和上面的 `boolean ensembleTracker = ...` 变量（以及对应的 `ZOOKEEPER_ENSEMBLE_TRACKER_KEY` import）。`ensembleTracker(boolean)` 是 curator 5.x 才加到 `Builder` 上的方法，curator 2.x 没有这个方法，保留就编译不过。去掉它不影响功能（curator 2.x 没有 ensemble tracker 这个概念）。

2. **AOT registrar 去掉 `OldServiceInstance` 注册行**

   curator5 的 AOT 反射注册里有：
   ```java
   typeDescribers.add(buildTypeDescriberWithDeclared("org.apache.curator.x.discovery.ServiceInstance"));
   typeDescribers.add(buildTypeDescriberWithDeclared("org.apache.curator.x.discovery.details.OldServiceInstance"));
   ```
   `OldServiceInstance` 是 curator 5.x 独有的兼容类，curator 2.x 没有。curator2 版本删掉这一行（只是 AOT 反射元数据，不影响运行期）。

**pom.xml 关键点**：curator / zookeeper 版本**在模块本地 pin 死**，不走 BOM（BOM 的 `curator_version=5.9.0` / `zookeeper_version=3.7.2` 是给 curator5 用的，不能动）：

```xml
<properties>
  <curator2.version>2.12.0</curator2.version>
  <zookeeper34.version>3.4.5</zookeeper34.version>
</properties>
<dependencies>
  <dependency>
    <groupId>org.apache.curator</groupId>
    <artifactId>curator-framework</artifactId>
    <version>${curator2.version}</version>
    <exclusions><exclusion><groupId>org.apache.zookeeper</groupId><artifactId>zookeeper</artifactId></exclusion></exclusions>
  </dependency>
  <dependency>
    <groupId>org.apache.curator</groupId>
    <artifactId>curator-recipes</artifactId>
    <version>${curator2.version}</version>
    <exclusions><exclusion><groupId>org.apache.zookeeper</groupId><artifactId>zookeeper</artifactId></exclusion></exclusions>
  </dependency>
  <dependency>
    <groupId>org.apache.zookeeper</groupId>
    <artifactId>zookeeper</artifactId>
    <version>${zookeeper34.version}</version>
    <exclusions><!-- slf4j-log4j12, log4j, jline, io.netty --></exclusions>
  </dependency>
  <dependency><groupId>org.apache.dubbo</groupId><artifactId>dubbo-common</artifactId><version>${project.parent.version}</version></dependency>
  <dependency><groupId>org.apache.dubbo</groupId><artifactId>dubbo-native</artifactId><version>${project.parent.version}</version></dependency>
</dependencies>
```

**为什么其它代码能原样复制**：curator 2.x 和 5.x 的核心 API 在 `Curator5ZookeeperClient` 用到的部分完全一致——

| API | curator 2.x | curator 5.x |
|-----|:---:|:---:|
| `CuratorFrameworkFactory.builder()` 链（connectString/retryPolicy/connectionTimeoutMs/sessionTimeoutMs/authorization/aclProvider/build） | ✓ | ✓ |
| `client.create().forPath()` / `.withMode(CreateMode.EPHEMERAL).forPath()` | ✓ | ✓ |
| `client.getData()` / `.setData()` / `.delete().deletingChildrenIfNeeded()` / `.getChildren().usingWatcher()` / `.checkExists()` | ✓ | ✓ |
| `client.getData().storingStatIn(stat)` | ✓ | ✓ |
| `NodeCache` + `NodeCacheListener`（`getListenable().addListener()`, `start()`, `getCurrentData()`） | ✓ | ✓（5.x 里已 deprecated 但仍可用） |
| `ChildData`、`ConnectionStateListener`、`RetryNTimes`、`CuratorWatcher`、`ACLProvider` | ✓ | ✓ |
| `client.getConnectionStateListenable()`、`blockUntilConnected`、`getZookeeperClient().isConnected()`、`getZooKeeper().getSessionId()` | ✓ | ✓ |
| **`Builder.ensembleTracker(boolean)`** | ✗ | ✓（唯一差异） |

所以除 `ensembleTracker` 外，整份 `Curator5ZookeeperClient` 在 curator 2.x 下都能原样编译运行。

### 4.2 新增模块 2：`dubbo-zookeeper-curator2-spring-boot-starter`

位置：`dubbo-spring-boot-project/dubbo-spring-boot-starters/dubbo-zookeeper-curator2-spring-boot-starter/`

和 `dubbo-zookeeper-curator5-spring-boot-starter` 对称，是个依赖打包模块（无 src）：

```xml
<dependencies>
  <dependency><groupId>org.apache.dubbo</groupId><artifactId>dubbo-remoting-zookeeper-curator2</artifactId><version>${project.version}</version></dependency>
  <dependency>
    <groupId>org.apache.curator</groupId><artifactId>curator-x-discovery</artifactId><version>${curator2.version}</version>
    <exclusions><!-- zookeeper, curator-framework, curator-recipes --></exclusions>
  </dependency>
  <dependency>
    <groupId>org.apache.zookeeper</groupId><artifactId>zookeeper</artifactId><version>${zookeeper34.version}</version>
    <exclusions><!-- slf4j-log4j12, log4j, jline, io.netty, logback --></exclusions>
  </dependency>
</dependencies>
```

它把 curator2 impl + `curator-x-discovery 2.12.0`（registry 的服务发现要用）+ `zookeeper 3.4.5` 一起带进来。

### 4.3 注册到 reactor

- `dubbo-remoting/pom.xml` 的 `<modules>` 加 `<module>dubbo-remoting-zookeeper-curator2</module>`
- `dubbo-spring-boot-project/dubbo-spring-boot-starters/pom.xml` 的 `<modules>` 加 `<module>dubbo-zookeeper-curator2-spring-boot-starter</module>`

现有 curator5 模块、BOM 一行未动。

---

## 5. Curator 2.x 的一个行为差异（无害，已记录）

测试时发现：curator 2.x 的 `CuratorFrameworkFactory.Builder` 在构造时把 **`defaultData` 字段默认初始化为本机地址字节**（`InetAddress.getLocalHost().getHostAddress()`，源码里是个静态 `LOCAL_ADDRESS` 字段）。所以 `create().forPath(path)`（不带 data）会把本机地址写进节点；curator 5.x 则写空 data。

**对 Dubbo 无害**：Dubbo 里「无 data 的 `createPersistent/createEphemeral`」只用于结构性父节点（如 `/dubbo`、`/dubbo/服务名/providers` 这些目录节点），其内容从不被读取；真正带数据的节点（provider 实例 URL 等）走 `createOrUpdate(path, data, ...)` 显式写数据。所以这个差异不影响功能。

---

## 6. 测试

`dubbo-remoting/dubbo-remoting-zookeeper-curator2/src/test/.../Curator5ZookeeperClientRealZkTest.java`

不连外部 ZK 进程，而是 **在测试进程内启动真实 ZK 3.4.5 服务端**：用 maven 仓库里的 `org.apache.zookeeper:zookeeper:3.4.5` jar，通过 `ZooKeeperServer` + `NIOServerCnxnFactory` 在随机端口起一个 standalone 服务端（`@BeforeAll` 起、`@AfterAll` 停）。这是真实的 3.4.5 服务端实现代码，和 `zkServer.sh` 跑的 3.4.5 是同一套，证明 curator2 client 正确说 ZK 3.4.x 线协议。

日志：ZK 3.4.5 用 slf4j API，由模块的 `log4j-slf4j-impl`（log4j2）test 依赖绑定到 log4j2，不需要 log4j 1.x。

8 个用例，全过：

| 用例 | 验证 |
|------|------|
| `createPersistentAndCheckExists` | 建持久节点 + checkExists |
| `createEphemeralWithCheckExists` | 建临时节点 |
| `createOrUpdateChangesContent` | createOrUpdate 写/改数据，getContent 读回 v1→v2 |
| `getChildren` | 列子节点 |
| `getConfigItemReturnsStat` | getConfigItem 带 Stat（`getTicket()`） |
| `childListenerFiresOnChildAdded` | CuratorWatcher 子节点变更回调 |
| `dataListenerFiresOnDataChange` | NodeCache 数据变更回调（NodeDataChanged） |
| `deleteRemovesNode` | 删节点 |

---

## 7. 下游使用方式

### 7.1 前提：应用用 dubbo 独立模块（不是聚合 jar）

drop-in 的机制是「classpath 上 curator5 jar 换成 curator2 jar」。所以应用必须是用各 dubbo 独立模块拼的，**不能**依赖 `org.apache.dubbo:dubbo`（聚合 fat jar）或 `dubbo-spring-boot-starter`（它间接拉聚合 jar）——聚合 jar 里 curator5 的类已经被 shade 打包进去，exclude 不了。聚合 jar 的情况见 §8。

### 7.2 Maven 配置

三个 zookeeper 消费模块各排掉 curator5 impl + curator/zookeeper（避免和 curator2 starter 的 2.12.0/3.4.5 版本冲突），再加 curator2 starter：

```xml
<dependency>
  <groupId>org.apache.dubbo</groupId>
  <artifactId>dubbo-registry-zookeeper</artifactId>
  <version>3.3.7-xiaomi-SNAPSHOT</version>
  <exclusions>
    <exclusion><groupId>org.apache.dubbo</groupId><artifactId>dubbo-remoting-zookeeper-curator5</artifactId></exclusion>
    <exclusion><groupId>org.apache.curator</groupId><artifactId>curator-x-discovery</artifactId></exclusion>
    <exclusion><groupId>org.apache.curator</groupId><artifactId>curator-framework</artifactId></exclusion>
    <exclusion><groupId>org.apache.curator</groupId><artifactId>curator-recipes</artifactId></exclusion>
    <exclusion><groupId>org.apache.zookeeper</groupId><artifactId>zookeeper</artifactId></exclusion>
  </exclusions>
</dependency>
<!-- dubbo-metadata-report-zookeeper、dubbo-configcenter-zookeeper 同样配 5 个 exclusion -->

<dependency>
  <groupId>org.apache.dubbo</groupId>
  <artifactId>dubbo-zookeeper-curator2-spring-boot-starter</artifactId>
  <version>3.3.7-xiaomi-SNAPSHOT</version>
</dependency>
```

应用代码 / 配置不变（`dubbo.registry.address=zookeeper://...` 照常）。三个消费模块的 `import ...curator5.*` 会落到 curator2 starter 带进来的 `dubbo-remoting-zookeeper-curator2` jar 里的同名类。

---

## 8. 聚合 jar 路线的已知问题（待处理）

如果应用走 `dubbo-spring-boot-starter` 或直接依赖 `org.apache.dubbo:dubbo`（聚合 fat jar）：

- `dubbo-distribution/dubbo-all` 用 `maven-shade-plugin` 把 `dubbo-remoting-zookeeper-curator5` 的类**直接打进 fat jar**（pom 里 shade `<include>` 了它，但没 include curator2）。
- 所以聚合 jar 里永远是 curator5 的 `Curator5ZookeeperClient`——它有 `ensembleTracker(boolean)` **直调**。
- 应用 classpath 上若 curator < 5（2.x 或 4.x），运行期会 `NoSuchMethodError: Builder.ensembleTracker(boolean)`。

curator2 drop-in 在聚合 jar 路线下**不生效**（curator2 的类没被 shade 进聚合 jar，且聚合 jar 里 curator5 的类 exclude 不了）。要解决有两条路（暂搁置）：

- **方案 1**：改 curator5 源码，把 `Curator5ZookeeperClient` 里的 `ensembleTracker` 直调改成反射（和 `CuratorFrameworkUtils` 已有的反射写法一致）。聚合 jar 里的类就兼容 curator 2/4/5 任意版本。改动 2 行，但动了 curator5 源码。
- **方案 2**：改 `dubbo-all/pom.xml` 的 shade 配置，把 `dubbo-remoting-zookeeper-curator5` 换成 `dubbo-remoting-zookeeper-curator2`。聚合 jar 变成 curator2 专版，不碰任何 java 源码，curator2 drop-in 真正生效。

---

## 9. 改动清单

| 文件 / 模块 | 操作 |
|------|------|
| `dubbo-remoting/dubbo-remoting-zookeeper-curator2/` | **新增**：curator5 impl 的 drop-in curator2 版（同包名同类名），9 个 java + 1 个 SPI 资源 + pom |
| `dubbo-spring-boot-project/dubbo-spring-boot-starters/dubbo-zookeeper-curator2-spring-boot-starter/` | **新增**：依赖打包 starter，pom |
| `dubbo-remoting/pom.xml` | 加一行 `<module>dubbo-remoting-zookeeper-curator2</module>` |
| `dubbo-spring-boot-project/dubbo-spring-boot-starters/pom.xml` | 加一行 `<module>dubbo-zookeeper-curator2-spring-boot-starter</module>` |
| `dubbo-remoting/dubbo-remoting-zookeeper-curator5/**` | **未动** |
| `dubbo-dependencies-bom/pom.xml` | **未动**（curator_version 仍 5.9.0、zookeeper_version 仍 3.7.2） |
| 三个 zookeeper 消费模块 | **未动** |

版本：`curator2.version=2.12.0`、`zookeeper34.version=3.4.5`，pin 在两个新模块的 pom 里。

---

## 10. 构建 / 验证命令

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home
cd /Users/cmj/IdeaProjects/dubbo

# 只编 curator2 impl（含上游依赖）
mvn -pl dubbo-remoting/dubbo-remoting-zookeeper-curator2 -am compile -Dmaven.test.skip=true -Dspotless.skip=true -Drat.skip=true -Dcheckstyle.skip=true

# 跑 curator2 对真实 ZK 3.4.5 的集成测试
mvn -pl dubbo-remoting/dubbo-remoting-zookeeper-curator2 test -Dtest=Curator5ZookeeperClientRealZkTest -Dspotless.skip=true -Drat.skip=true -Dcheckstyle.skip=true

# 全 reactor clean install
mvn clean install -Dmaven.test.skip=true -Dspotless.skip=true -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Djacoco.skip=true -Dskip_maven_deploy=true
```
