# Auto Tag Routing from First Argument — 设计方案

> 在 `@DubboReference` 上增加一个默认关闭的特性:开启后,RPC 调用时自动把(第一个)参数转成字符串,作为请求 tag(`dubbo.tag`)注入,供 `TagStateRouter` 做 tag 路由。

## 1. 调用链定位(为什么是 ClusterFilter)

`dubbo-rpc/dubbo-rpc-api/src/main/java/org/apache/dubbo/rpc/Filter.java` 注释明确写了 consumer 侧调用顺序:

```
Proxy → ClusterFilter → ClusterInvoker(路由+LB) → Filter → Invoker(真正发 RPC)
```

- 普通 consumer `Filter` 在 `ClusterInvoker` **之后**跑,设 tag 时 `TagStateRouter` 已经路由完了 → 太晚,不能用。
- `ClusterFilter` 在 `ClusterInvoker` **之前**跑。`TagStateRouter.doRoute` 读的是 `invocation.getAttachment(TAG_KEY)`(`TagStateRouter.java:110`),同一个 `invocation` 对象从 ClusterFilter 一路传到 router,所以 ClusterFilter 里 `setAttachment(TAG_KEY, ...)` router 能看到 → 正确切入点。
- 已有 `ConsumerContextFilter`(`@Activate(group=CONSUMER, order=Integer.MIN_VALUE)`)就是 ClusterFilter,证明这条路径可行。

## 2. 配置面(`@DubboReference` → consumer URL)

把这一组语义相关的配置收成一个独立注解 `@AutoTag`,再以**嵌套成员**形式挂到 `@DubboReference`,避免 `@DubboReference`(已 30+ 属性)继续膨胀,且 `@AutoTag` 后续可对称复用到 `@DubboService`。

> 选"嵌套"而非"独立贴在 field 旁边":独立形态需要单独写 BeanPostProcessor 把 `@AutoTag` 绑定到对应 `ReferenceConfig`,多 reference 场景下绑定关系易出错,plumbing 重。嵌套形态复用 `@DubboReference` 既有生命周期,只需在 `appendConfig` 手动透传值。

### 新注解 `@AutoTag`

```java
// dubbo-common/.../config/annotation/AutoTag.java
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface AutoTag {
    /** 是否开启按参数自动注入 tag(dubbo.tag),默认关闭 */
    boolean enabled() default false;

    /** tag 来源参数下标,默认 0(第一个参数) */
    int index() default 0;

    /**
     * 无匹配 tagged provider 时是否强制失败(设 force.tag=true)。
     * 默认 true:快速失败,避免静默 fallback 到无 tag provider 造成跨 tag 路由(如跨租户)。
     * 设 false 则遵循 TagStateRouter 默认行为,无匹配时 fallback 到无 tag provider。
     */
    boolean force() default true;
}
```

### `@DubboReference` 引用

```java
// DubboReference.java 新增一个成员
/**
 * 按参数自动 tag 路由配置,默认 @AutoTag(enabled=false) 即关闭。
 * @see AutoTag
 */
AutoTag autoTag() default @AutoTag;
```

使用方式:

```java
// 不开启(默认)
@DubboReference
private OrderService orderService;

// 开启,默认 force=true
@DubboReference(autoTag = @AutoTag(enabled = true))
private OrderService orderService;

// 开启,关闭 force(无匹配时 fallback 而非失败)
@DubboReference(autoTag = @AutoTag(enabled = true, force = false))
private OrderService orderService;

// 用第 2 个参数(index=1)作 tag
@DubboReference(autoTag = @AutoTag(enabled = true, index = 1))
private OrderService orderService;
```

### 透传链路(嵌套注解需一小块 plumbing)

`@DubboReference` 现有的"注解属性 → `ReferenceConfig` setter → URL 参数"靠**同名反射拷贝**;嵌套的 `@AutoTag` 不会被自动识别,需在 `ReferenceConfig.appendConfig()` 手动读一次,塞 3 个值进 URL 参数:

```java
// ReferenceConfig.appendConfig() 里追加
AutoTag autoTag = getAutoTag();   // 从注解 / config 字段取
if (autoTag != null && autoTag.enabled()) {
    referenceParameters.put(AUTO_TAG_FROM_ARG_KEY, String.valueOf(autoTag.enabled()));
    referenceParameters.put(AUTO_TAG_ARG_INDEX_KEY, String.valueOf(autoTag.index()));
    referenceParameters.put(AUTO_TAG_FORCE_KEY, String.valueOf(autoTag.force()));
}
```

URL parameters:

- `auto.tag.from.arg` = true/false(`enabled`,同时作 `@Activate(key=...)` 激活键)
- `auto.tag.arg.index` = 0
- `auto.tag.force` = true/false

3 个都是基本类型,`getParameter` 取值廉价,无需 attributes/Set 预解析。consumer URL 一路到 Invoker,`AutoTagClusterFilter` 从 `invoker.getUrl()` 读取。

## 3. 核心实现:AutoTagClusterFilter

新建 `dubbo-cluster/.../filter/support/AutoTagClusterFilter.java`:

```java
@Activate(group = CONSUMER, value = AUTO_TAG_FROM_ARG_KEY, order = Integer.MIN_VALUE + 1000)
public class AutoTagClusterFilter implements ClusterFilter, ClusterFilter.Listener {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        URL url = invoker.getUrl();

        // @Activate(key=...) 已保证只有开启的 reference 才挂这个 filter,这里二次确认
        if (!url.getParameter(AUTO_TAG_FROM_ARG_KEY, false)) {
            return invoker.invoke(inv);
        }

        // 规则 1:调用方已显式设了 tag → 尊重显式,不覆盖
        if (StringUtils.isNotEmpty(inv.getAttachment(TAG_KEY))) {
            return invoker.invoke(inv);
        }

        // 取参数
        int idx = url.getParameter(AUTO_TAG_ARG_INDEX_KEY, 0);
        Object[] args = inv.getArguments();
        if (args == null || idx < 0 || idx >= args.length) {
            return invoker.invoke(inv);   // 越界 → 跳过,走正常路由
        }
        Object src = args[idx];
        if (src == null) {
            return invoker.invoke(inv);   // null → 跳过
        }
        String tag = String.valueOf(src);
        if (StringUtils.isEmpty(tag)) {
            return invoker.invoke(inv);
        }

        // 注入 tag,TagStateRouter 随后读到
        if (inv instanceof RpcInvocation) {
            RpcInvocation rpcInv = (RpcInvocation) inv;
            rpcInv.setAttachment(TAG_KEY, tag);
            // force:默认 true(从 URL 读),无匹配 tagged provider 时快速失败,避免静默跨 tag 路由
            boolean force = url.getParameter(AUTO_TAG_FORCE_KEY, true);
            rpcInv.setAttachment(FORCE_USE_TAG, String.valueOf(force));
        }
        return invoker.invoke(inv);
    }

    @Override public void onResponse(Result appResponse, Invoker<?> invoker, Invocation inv) {}
    @Override public void onError(Throwable t, Invoker<?> invoker, Invocation inv) {}
}
```

SPI 注册(`META-INF/dubbo/internal/org.apache.dubbo.rpc.cluster.filter.ClusterFilter`):

```
auto-tag=org.apache.dubbo.rpc.cluster.filter.support.AutoTagClusterFilter
```

新增常量(放 `CommonConstants` 或新 `AutoTagConstants`):

```java
String AUTO_TAG_FROM_ARG_KEY = "auto.tag.from.arg";
String AUTO_TAG_ARG_INDEX_KEY = "auto.tag.arg.index";
String AUTO_TAG_FORCE_KEY = "auto.tag.force";
```

## 4. 关键设计决策

| 决策 | 选择 | 理由 |
|---|---|---|
| 切入点 | ClusterFilter | 唯一"早于路由"的 SPI 点;普通 Filter 太晚 |
| 生效范围 | `@Activate(value=AUTO_TAG_FROM_ARG_KEY)` | 只挂在开启的 reference 上(消费者 URL 含 `auto.tag.from.arg`),未开启的 reference 零开销(连 filter 都不在链里) |
| 显式 tag 优先 | 已设 tag 不覆盖 | 允许调用方对单次调用手动覆盖(`RpcContext...setAttachment(TAG_KEY,...)`) |
| 参数为 null/越界 | 跳过,不报错 | 不影响正常调用;落到 TagStateRouter 的"无 tag"分支(走无 tag provider) |
| 类型转换 | `String.valueOf(arg)` | v1 简单;非 String(如 Long tenantId)也能用 |
| 注入位置 | `RpcInvocation.setAttachment` | per-call 对象,调用结束即丢弃,无线程泄漏 |
| force 默认值 | `@AutoTag(force=true)` | 特性语义是按参数隔离路由,无匹配时快速失败比静默 fallback 更安全;可关 |
| 配置形态 | 独立 `@AutoTag` 注解,嵌套挂到 `@DubboReference.autoTag()` | 集中管理、可复用(后续可对称加 `@DubboService`);只多一块 `appendConfig` 透传 plumbing |
| 方法过滤 | v1 不做 | 去掉 includes/excludes,所有方法都自动注入;后续如需再按方法过滤,优先考虑 attributes 存 Set 而非展开参数 |

## 5. 与现有 tag 机制的交互

- **`dubbo.tag` URL 参数(provider 侧静态 tag)**:不变,本特性只是 consumer 侧自动设请求 tag,provider 标签匹配逻辑全在 `TagStateRouter`。
- **`force.tag`**:本特性默认设 `force.tag=true`(`@AutoTag(force=true)` 默认),无匹配 tagged provider 时快速失败,避免静默 fallback 跨 tag 路由。可通过 `@AutoTag(force=false)` 关闭,回落到 TagStateRouter 默认的"无匹配则 fallback 无 tag provider"行为。force 取值由 filter 写入 `FORCE_USE_TAG` 附件,`TagStateRouter.isForceUseTag`(L254-257)读取。
- **`dubbo.backup.tag`**:不涉及。
- **动态 tag-router 规则(配置中心)**:不涉及,本特性只负责"产 tag",路由规则仍由现有 TagRouterRule 管理。

## 6. 文件改动清单

1. `dubbo-common/.../config/annotation/AutoTag.java` — **新建**注解(`enabled` / `index` / `force`)
2. `dubbo-common/.../config/annotation/DubboReference.java` — 加一个成员 `AutoTag autoTag() default @AutoTag;`
3. `dubbo-common/.../constants/CommonConstants.java`(或新建 `AutoTagConstants`)— 加 3 个常量(`AUTO_TAG_FROM_ARG_KEY` / `AUTO_TAG_ARG_INDEX_KEY` / `AUTO_TAG_FORCE_KEY`)
4. `dubbo-config/dubbo-config-api/.../ReferenceConfig.java` — 加 `autoTag` 字段 + getter/setter;`appendConfig()` 透传 3 个 string 参数
5. `dubbo-cluster/.../filter/support/AutoTagClusterFilter.java` — **新建**
6. `dubbo-cluster/src/main/resources/META-INF/dubbo/internal/org.apache.dubbo.rpc.cluster.filter.ClusterFilter` — 加一行 SPI
7. 测试:`AutoTagClusterFilterTest`(单测 filter 逻辑)+ `ReferenceConfig` 透传单测 + 集成验证 tag 路由命中

## 7. 测试要点

- 开启(`@AutoTag(enabled=true)`)+ 第一个参数有值 → `invocation.getAttachment(TAG_KEY)` == 参数字符串,且实际路由到带该 tag 的 provider
- 不开启(默认或未贴 `@AutoTag`)→ filter 不在链中,`getAttachment(TAG_KEY)` 为 null
- 第一个参数为 null → 不设 tag,不抛异常
- 调用方手动 `RpcContext...setAttachment(TAG_KEY,"x")` + 开启 → tag 仍为 "x"(显式优先)
- `@AutoTag(index=...)` 越界 → 跳过
- 非 String 参数(如 `Long 1001L`)→ tag = "1001"
- `@AutoTag(force=true)`(默认)+ 无匹配 tagged provider → 调用失败(不 fallback)
- `@AutoTag(force=false)` + 无匹配 → fallback 到无 tag provider
- `ReferenceConfig.appendConfig()` 透传:`@AutoTag(enabled=true,index=1,force=false)` → URL 含 `auto.tag.from.arg=true` / `auto.tag.arg.index=1` / `auto.tag.force=false`

## 8. 待拍板的问题

1. **参数类型**:v1 用 `String.valueOf`。如果第一个参数常是 POJO(需要取某个字段),要不要一并设计一个 `AutoTagExtractor` SPI(按接口/方法配 extractor)?还是 v1 只支持基本类型/String?
2. **`@AutoTag.force` 默认值**:已定默认 `true`(force 开,无匹配快速失败)。若你的场景更看重容错(新 tag 值尚无 provider 时不要报错),可改为默认 `false`。
3. **作用域**:只在 `@DubboReference` 加 `autoTag()`,还是 `@DubboService` 侧也对称加(一般不需要,tag 路由是 consumer 侧产 tag)?`@AutoTag` 注解独立后,对称扩展成本低。
4. **方法过滤**:v1 已去掉 includes/excludes,所有方法都自动注入。后续如需按方法过滤,设计上倾向 attributes 存 Set(而非展开成 N 个 URL 参数)。
