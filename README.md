# Raknetify — 中国大陆网络适配分支

**简体中文** | [English](README.en.md)

本项目是 [RelativityMC/raknetify](https://github.com/RelativityMC/raknetify) 的非官方分支。感谢上游作者及贡献者提供的 RakNet 多通道传输基础。**使用本分支遇到问题，请提交到[本仓库 Issues](https://github.com/ERroooooR/raknetify/issues)，不要直接向原项目提交本分支的问题。**

> **实验性与 AIGC 声明：** 本仓库的分支提交包含大量 AIGC（人工智能生成内容）及 AI 辅助编写的代码、文档。无法保证最新构建的稳定性；构建成功或存在自动化测试不代表实际联机环境稳定。

## 项目目标与支持范围

上游面向不可靠、受限网络下的 Minecraft 联机体验；本分支的目标与上游不同，主要针对**中国大陆地区的 QoS、限包速、丢包、抖动与突发流量情况**修改和调优。无法确保这些策略具有普适性，也不保证在其他地区、运营商或线路上改善性能。

**本分支仅确保 NeoForge 环境下与信雅互联（Sinytra Connector）的兼容性。** 这里指通过 Connector 加载 Fabric 版 Raknetify，并非提供原生 NeoForge 模组。这一兼容范围不等于承诺所有 Minecraft / NeoForge / Connector 版本、整合包或最新构建都稳定可用。

仓库仍保留 Fabric、Velocity、BungeeCord 等上游模块及部分兼容代码，但它们的存在不代表本分支对这些独立环境作出兼容性保证。

## 已完成的改进

以下内容依据当前代码及仓库固定的 `netty-raknet` 子模块整理，描述已实现的机制，不代表已证明在所有网络中有效：

- **NeoForge / Connector 适配：** 增加 Connector 专用服务端绑定与通道初始化处理，调整加密相关注入，并修正与 Fabric Networking API 的部分 Mixin 交互。
- **自适应传输：** 默认启用包速率与字节节奏控制、滚动丢包分类、拥塞窗口控制及有界突发排空；保留空闲前已验证的路径容量，减少恢复发送时不必要的速率回落。
- **协议与 MTU：** 默认优先 RakNet v12，在旧端拒绝初始请求时回退 v11；通过协商使用路径 MTU 探测（DPLPMTUD）及有界 Reed–Solomon FEC，并提供 MTU 回退。旧协议不会发送 v12 扩展包。
- **丢包与有序队列恢复：** 实现短乱序 NACK 宽限、按需 ACK/NACK 重复保护、RACK 风格丢包推断、PTO 探测、应用流量受限时的额外恢复，以及针对有序队列阻塞的定向修复和对端反馈。额外恢复受发送预算及拥塞条件约束。
- **压缩兼容：** 增加 BandwidthOptimizer 与 ZSTD_Compresser 的检测和管线适配，处理重复压缩、压缩协商及 Velocity 侧冗余长度前缀；详见下方限制。
- **可观测性：** 增加双向传输指标、分通道队头阻塞诊断、压缩批次指标及异步 JSONL 导出，便于区分丢包、排队与压缩批次造成的延迟。
- **连接处理：** 增加 Gate 路由提示，修正 IPv6 地址处理及部分握手、关闭和分片同步行为。

实现入口：[传输配置](common/src/main/java/com/ishland/raknetify/common/connection/RakNetConnectionUtil.java)、[Fabric 适配](fabric/src/main/java/com/ishland/raknetify/fabric/mixin)、[传输子模块](netty-raknet)、[恢复设计与开关](docs/ADAPTIVE_RECOVERY_ROADMAP.md)。设计文档包含演进方向，不能将所有设想视为已完成的性能验证。

## 安装与连接

1. 从**本仓库**的 [Releases](https://github.com/ERroooooR/raknetify/releases)（如有发布）或 [Actions 构建产物](https://github.com/ERroooooR/raknetify/actions/workflows/build.yml)获取对应版本。上游下载渠道不代表本分支构建。
2. 在 NeoForge 中通过信雅互联加载适配当前 Minecraft 版本的 Fabric 产物，并安装该 Connector 版本要求的依赖。不要仅凭上游的历史版本范围判断本分支兼容性。
3. 直连时客户端和服务端均需安装；使用代理时，RakNet 终止于安装了对应插件的代理，后端通常无需安装。代理部署属于本分支不保证兼容的环境。
4. 在服务端或代理放行与 Minecraft TCP 端口**相同端口号的 UDP 端口**，并确保 NAT / 转发链路支持 UDP。
5. 使用 `raknet;example.com` 连接。`raknetl;` 会请求高 MTU，不适合作为未经验证线路的默认选择。

## 兼容性与已知限制

| 场景 | 行为与限制 |
| --- | --- |
| NeoForge + 信雅互联 | 本分支唯一承诺的兼容方向；仍需匹配游戏、加载器、Connector 和依赖版本。 |
| 原生 Fabric / Velocity / BungeeCord / ViaVersion | 保留上游实现或相关适配，但不保证本分支兼容；代理遇到不支持的客户端版本时，多通道可能无法初始化，响应性下降。 |
| 旧版 RakNet 对端 | 可按握手结果回退到 v11；v12 扩展能力需要协商，回退不代表保留所有优化。 |
| BandwidthOptimizer | 默认检测后由其负责压缩，关闭 RakNet 连接的流式 Deflate、原版压缩和其延迟批处理，以保留逐包优先级；TCP 连接不受这些调整影响。 |
| ZSTD_Compresser | 检测 `zstd_compresser` 客户端与 `zstd_velocity` 插件，关闭流式 Deflate，保留必要的 `SetCompression` 协商，并在 Velocity 移除冗余 TCP 长度前缀。合批期间使用单个有序通道，失去原始逐包多通道优先级，仍可能出现队头阻塞。 |
| 同时安装两种压缩方案 | 代码优先保留 ZSTD_Compresser 所需的协商；这不构成对任意组合或版本的兼容保证。 |
| UDP / QoS / DSCP | UDP 被阻断时无法建立 RakNet 连接；DSCP 标记不保证被运营商保留或优待，MTU、FEC 和速率调优无法消除所有限速或丢包。 |

ZSTD_Compresser 的大批次可能增加分片等待。可在客户端和 Velocity 的外部压缩配置中对比测试 `batch_max_bytes: 32768`、`flush_interval_ms: 5`；这是测试起点，不是通用最优值，也不是 Raknetify JVM 参数。

## 配置与诊断

以下为当前代码默认值；参数加到相应客户端、服务端或代理的 JVM 启动参数中：

| JVM 属性 | 默认值 | 用途 |
| --- | --- | --- |
| `raknetify.adaptiveTransport` | `true` | 自适应传输总开关 |
| `raknetify.protocolVersion` | `12` | 首选协议版本，可配置 9–12 |
| `raknetify.adaptiveMinPps` | `30` | 自适应包速率下限 |
| `raknetify.adaptiveMaxPps` | `2000` | 自适应包速率上限 |
| `raknetify.smallWriteCoalesceMicros` | `500` | 小写入合并时间（微秒） |
| `raknetify.plpmtudMaxMtu` | `1452` | 探测的本地 UDP 载荷上限；初始普通 MTU 为 1400 |
| `raknetify.adaptiveDscp` | `false` | 共享 UDP socket 的自适应 DSCP 投票切换 |
| `raknetify.metricsJsonl` | `false` | 每连接每秒导出诊断指标 |

例如启用日志：

```text
-Draknetify.metricsJsonl=true
```

日志位于游戏或代理工作目录的 `logs/raknetify-metrics.jsonl`。记录包含 RTT、吞吐、队列、重传、FEC、MTU、分通道阻塞和压缩批次等信息；异步写入队列满时会丢弃指标并记录 `export_dropped`。

`raknetify.adaptiveAckProtection`、`raknetify.adaptiveNackGrace`、`raknetify.adaptiveNackProtection` 默认开启，可分别设为 `false` 做对照测试。高级恢复开关见[恢复文档](docs/ADAPTIVE_RECOVERY_ROADMAP.md)。压缩适配可用 `-Draknetify.bandwidthOptimizerCompatibility=false` 或 `-Draknetify.zstdCompresserCompatibility=false` 在两端关闭以排查问题。

提交 Issue 时请附上构建提交号、Minecraft / NeoForge / Connector 版本、模组与代理列表、网络拓扑、复现步骤及相关日志或指标片段；分享前移除 IP、令牌等敏感信息。

## 许可证与致谢

沿用 [MIT 许可证](LICENSE)。感谢 [RelativityMC/raknetify](https://github.com/RelativityMC/raknetify) 及 netty-raknet 的作者和贡献者；本分支的修改与支持承诺由本仓库负责。
