# TagStateRouter 备用标签路由改造方案

> Draft · 待评审 · 范围：仅静态 URL 参数标签 · 方案 B（保留 force.tag 守卫）

## 01 · 需求

一台 Provider 实例打两个标签：**主标签** `dubbo.tag = online`，**备用标签** `dubbo.backup.tag = preview,gray`（逗号分隔多值，表示该实例同时是 `preview` 和 `gray` 的备用）。客户端请求 `dubbo.tag = preview` 时，先匹配所有实例的 `dubbo.tag`；若全部 miss，再匹配 `dubbo.backup.tag` 列表中是否包含 `preview`。

关键语义：备用标签回退是**无条件**的——主标签查不到 → 备用标签，不受 `force.tag` 控制。`force.tag` 只守卫最后的「退化到无标签实例」这一层，语义与原代码完全一致：`force.tag=true` 时备用也 miss 返回空；`force.tag=false`（默认）时备用 miss 退化到无标签实例。

> 备用标签支持逗号分隔多值，例如 `dubbo.backup.tag = preview,gray`；主标签 `dubbo.tag` 仍为单值（与现状一致，不改）。

## 02 · 范围决策：只支持静态 tag

经讨论确认，备用标签**只走静态 U  RL 参数**，不扩展动态规则模型。含义：

- 备用标签只通过 Provider URL 上的 `dubbo.backup.tag` 参数表达，不进入配置中心的 `TagRouterRule` / `Tag` / `ParamMatch` 模型。
- 改动**只动 `filterUsingStaticTag` 一处**，`doRoute` 的动态路径（含 FAILOVER）完全不碰。
- 副作用：当配置中心存在**有效**的动态 tag 规则时，`doRoute` 走动态路径并在内部返回，**不会**落到 `filterUsingStaticTag`，因此备用标签在该场景下不生效。这对「只用静态标签、不配动态规则」的用法无影响——本方案的目标用户即此类。

## 03 · 现状：为什么不支持

当前 `TagStateRouter` 全程只认 `TAG_KEY = "dubbo.tag"` 一个字段，静态路径的回退分支退化到「URL 上无 `dubbo.tag` 的实例」，而非「备用标签命中的实例」。

### 静态路径 `filterUsingStaticTag`（:210）

```java
// TagStateRouter.java
if (!StringUtils.isEmpty(tag)) {
    result = filterInvoker(invokers,
        invoker -> ANY_VALUE.equals(tag) || tag.equals(invoker.getUrl().getParameter(TAG_KEY)));
    // 主 miss 后的回退：选「没有任何 dubbo.tag」的实例
    if (CollectionUtils.isEmpty(result) && !isForceUseTag(invocation)) {
        result = filterInvoker(invokers,
            invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
    }
} else {
    result = filterInvoker(invokers, invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
}
```

> **问题**：一个 `dubbo.tag=online, dubbo.backup.tag=preview` 的实例，请求 `preview` 主匹配 miss 后会被这条回退**排除**——因为它有 `dubbo.tag`（非 empty），不会进入「无标签」集合，也不会被当作备用候选保留。

## 04 · 设计目标

- **两级标签匹配**：主标签 `dubbo.tag` → 备用标签 `dubbo.backup.tag` → 无标签实例 / 空。
- **备用回退无条件**：主 miss → 备用不受 `force.tag` 控制，始终尝试备用标签。
- **保留 force.tag 语义**：`force.tag` 只守卫最后的「退化到无标签」层，与原代码一致——`true` 严格返回空，`false` 默认退化。完全向后兼容。
- **最小侵入**：改动收敛在 `TagStateRouter.filterUsingStaticTag` 与一个新常量，不动 `TagRouterRule` / `Tag` / `ParamMatch` 模型，不动 `doRoute`。
- **静态专属**：备用标签是 Provider URL 静态参数；动态规则激活时不参与（见 §02）。

## 05 · 改造后的路由流程

以「请求带 tag」为前提（无 tag 的请求走原有「返回无标签实例」逻辑，保持不变）：

1. **匹配 `dubbo.tag == 请求tag`** — 遍历 invokers，筛 URL 主标签等于请求 tag 的实例。
2. **主标签为空 → 匹配 `dubbo.backup.tag` 包含请求 tag** — 无条件进入，不受 `force.tag` 影响。备用标签为逗号分隔多值，用 `StringUtils.isContains(backupParam, tag)` 判断请求 tag 是否在列表中。
3. **备用也为空 → 按 `force.tag` 决定** — `force.tag=false`（默认）退化到「无标签」实例（与原代码一致）；`force.tag=true` 返回空集合。

无 tag 请求（请求方未指定 `dubbo.tag`）的出口不变：仍返回「URL 上无 `dubbo.tag`」的实例。

## 06 · 改造方案

### 6.1 新增常量

在 `CommonConstants` 中 `TAG_KEY` 旁新增备用标签键。

```java
// dubbo-common / CommonConstants.java · :350
String TAG_KEY = "dubbo.tag";
String BACKUP_TAG_KEY = "dubbo.backup.tag";   // 新增
```

### 6.2 改造 `filterUsingStaticTag`

把「主 miss → 无标签」改为「主 miss → 备用 → 空」。

**Before**

```java
if (!StringUtils.isEmpty(tag)) {
    result = filterInvoker(invokers,
        invoker -> ANY_VALUE.equals(tag) || tag.equals(invoker.getUrl().getParameter(TAG_KEY)));
    if (CollectionUtils.isEmpty(result) && !isForceUseTag(invocation)) {
        result = filterInvoker(invokers,
            invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
    }
} else {
    result = filterInvoker(invokers, invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
}
```

**After**

```java
if (!StringUtils.isEmpty(tag)) {
    result = filterInvoker(invokers,
        invoker -> ANY_VALUE.equals(tag) || tag.equals(invoker.getUrl().getParameter(TAG_KEY)));
    // 主标签 miss → 强制匹配备用标签 dubbo.backup.tag（逗号分隔多值）
    // 备用回退无条件：force.tag 不守卫这一层
    if (CollectionUtils.isEmpty(result)) {
        result = filterInvoker(invokers,
            invoker -> StringUtils.isContains(
                invoker.getUrl().getParameter(BACKUP_TAG_KEY), tag));
    }
    // 备用也 miss → 按 force.tag 决定是否退化到无标签实例
    // 保留原 isForceUseTag 守卫，语义与原代码一致
    if (CollectionUtils.isEmpty(result) && !isForceUseTag(invocation)) {
        result = filterInvoker(invokers,
            invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
    }
} else {
    result = filterInvoker(invokers, invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(TAG_KEY)));
}
```

> 在主匹配与「无标签退化」之间**插入**备用标签层，不删除原 `!isForceUseTag(invocation)` 守卫。请求带 tag 时走 主→备用→（force.tag 决定是否）无标签；请求不带 tag 时（`else` 分支）保持原行为。
>
> 备用标签用 `StringUtils.isContains(values, value)`：该方法内部按逗号 split 后判包含，是 Dubbo 既有惯用写法（见 `StringUtils.java:577`，基于 `COMMA_SPLIT_PATTERN`）。天然兼容单值与多值：`"preview"` 与 `"preview,gray"` 对请求 `preview` 都命中；空值或未设置返回 false。

## 07 · 兼容性影响

> **向后兼容**：方案 B 保留原 `!isForceUseTag(invocation)` 守卫与「无标签退化」层，只在主匹配与该层之间插入备用标签层。`force.tag` 的语义和原代码完全一致——只控制最后的无标签退化。

存量行为对照：

| 场景 | 原行为 | 方案 B 行为 |
|------|--------|-------------|
| 主标签命中 | 返回主命中 | 返回主命中（不变） |
| 主 miss + `force.tag=false` | 退化到无标签 | 先备用；备用 miss 再退化到无标签（仅当无备用命中时退化，与原一致） |
| 主 miss + `force.tag=true` | 返回空 | 先备用；备用 miss 返回空（不变） |

唯一可观察差异：原本主 miss + `force.tag=false` 会直接退化到无标签；现在若存在备用命中则改返回备用命中。这是新增能力，非破坏——没有任何 Provider 设置 `dubbo.backup.tag` 时，行为与原代码完全相同。

`doRoute` 动态路径与其 FAILOVER **不改**，行为完全保持现状。

## 08 · 边界用例

- **双标签同值**：实例 `dubbo.tag=preview, dubbo.backup.tag=preview`。主匹配即命中，不走备用，行为正确。
- **主标签命中**：请求 `online`，实例 `dubbo.tag=online`。主匹配命中，备用层不触发。
- **仅备用命中（单值）**：请求 `preview`，实例 `dubbo.tag=online, dubbo.backup.tag=preview`。主 miss → 备用命中。
- **仅备用命中（多值）**：请求 `gray`，实例 `dubbo.tag=online, dubbo.backup.tag=preview,gray`。主 miss → 备用 `isContains` 命中。
- **全部 miss + `force.tag=false`**：请求 `preview`，无任何实例主或备用包含 `preview`，存在无标签实例。退化到无标签实例（与原代码一致）。
- **全部 miss + `force.tag=true`**：同上但请求带 `force.tag=true`。返回空集合，不退化。
- **空备用值**：实例 `dubbo.backup.tag` 未设置或为空字符串。`isContains` 返回 false，该实例不参与备用匹配。
- **通配 `*`**：在 `doRoute:112` 已提前返回全部；若直接进静态路径，`ANY_VALUE.equals(tag)` 亦匹配全部，不进入备用逻辑。
- **无 tag 请求**：请求方未设 `dubbo.tag`。`else` 分支返回无 `dubbo.tag` 的实例，行为不变。
- **动态规则激活**：配置中心存在有效动态 tag 规则时，`doRoute` 走动态路径并在内部返回，不进 `filterUsingStaticTag`，备用标签不生效（见 §02，by design）。
- **分级标签 `beta|team1`**：属于动态路径的地址匹配，静态备用标签不参与，两者独立。

## 09 · 测试用例

| 用例 | Provider 主/备用标签 | 请求 tag | 期望结果 |
|------|---------------------|----------|----------|
| T1 主命中 | `tag=online` | `online` | 命中该实例 |
| T2 备用命中（单值） | `tag=online, backup.tag=preview` | `preview` | 命中该实例 |
| T3 双命中取主 | `tag=preview, backup.tag=preview` | `preview` | 命中该实例（主） |
| T4 备用命中（多值） | `tag=online, backup.tag=preview,gray` | `gray` | 命中该实例 |
| T5 全 miss + force=false | A:`tag=online, backup.tag=gray` / B:无标签 | `preview` | 退化到 B（无标签） |
| T6 全 miss + force=true | 同 T5 | `preview` + `force.tag=true` | 空集合 |
| T7 多实例混合 | A:`tag=online` / B:`tag=online, backup.tag=preview,gray` | `preview` | 仅 B |
| T8 无 tag 请求 | 同 T7 | （无） | 无 `dubbo.tag` 的实例（若有） |
| T9 备用为空值 | `tag=online, backup.tag=`（空） | `online` 之外的任意 | 退化到无标签（force=false） |

## 10 · 改动文件清单

- **新增常量** — `dubbo-common / CommonConstants.java` · `BACKUP_TAG_KEY`
- **修改** — `dubbo-cluster / TagStateRouter.java` · `filterUsingStaticTag`
- **测试** — `dubbo-cluster / TagStateRouterTest` · 补充 T1–T9

---

*方案 B · 向后兼容 · 已落地并通过测试*
