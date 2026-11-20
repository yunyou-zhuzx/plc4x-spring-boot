# plc4x-spring-boot-starter

Spring Boot Starter for Apache PLC4X - 工业 PLC 通信框架

## 简介

`plc4x-spring-boot-starter` 是一个 Spring Boot 集成框架，基于 [Apache PLC4X](https://plc4x.apache.org/) 开发，提供简洁易用的方式来读写工业 PLC（可编程逻辑控制器）数据。

支持多种工业协议：

- **S7** - 西门子 PLC 通信协议
- **Modbus** - 通用工业通信协议
- **OPC UA** - 工业自动化通信标准

## 技术栈

- Java 17
- Spring Boot 3.2.3
- Apache PLC4X 0.13.1
- Lombok
- Hutool

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>com.yunyoucloud</groupId>
    <artifactId>plc4x-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. 配置 PLC 连接

在 `application.yml` 或 `application.properties` 中配置连接信息：

#### S7 连接配置（西门子 PLC）

```yaml
plc:
  s7:
    enable: true
    connections:
      plc1:
        enable: true
        ip: 192.168.10.73
        port: 102
        rack: 0
        slot: 1
        pduLength: 240
```

#### Modbus TCP 配置

```yaml
plc:
  modbus:
    enable: true
    connections:
      plc1:
        enable: true
        ip: 10.31.128.110
        port: 502
        slave: 1
```

#### OPC UA 配置

```yaml
plc:
  opcua:
    enable: true
    connections:
      plc3:
        enable: true
        ip: 10.31.128.115
        port: 4840
```

### 3. 启用 PLC 功能

在 Spring Boot 启动类或配置类上添加注解：

```java
@EnablePlc
@SpringBootApplication
public class MainApplication {
    public static void main(String[] args) {
        SpringApplication.run(MainApplication.class, args);
    }
}
```

### 4. 使用示例

#### 读取数据

```java
@Autowired
private PlcConfig plcConfig;

final PLC plc =  PlcManager.getPlc(plcConfig.getPlcConfigs()[0].getConnections().get("plc1"));
final IPLCSerializable iplcSerializable = IPLCSerializable.newInstance(plc);

// 读取单个值
boolean flag = iplcSerializable.readBoolean("M0.0");
int counter =iplcSerializable.readInteger("DB1.12");
float temperature = iplcSerializable.readFloat("DB1.12");

// 批量读取
final WarehouseReceiveDB read = iplcSerializable.read(WarehouseReceiveDB.class);
```

#### 写入数据

```java
@Autowired
private PlcConfig plcConfig;

final PLC plc =  PlcManager.getPlc(plcConfig.getPlcConfigs()[0].getConnections().get("plc1"));
final IPLCSerializable iplcSerializable = IPLCSerializable.newInstance(plc);


// 写入单个值
iplcSerializable.writeBoolean("M0.0", true);
iplcSerializable.writeInteger("DB1.DBD0", 100);

// 批量写入
final WarehouseSendDB warehouseSendDB = new WarehouseSendDB();
warehouseSendDB.setMaterialName("电箱柜");
iplcSerializable.write(warehouseSendDB);
```

## 核心组件

| 组件 | 说明 |
|------|------|
| `PlcManager` | PLC 连接管理器，负责创建和管理 PLC 连接 |
| `PLC` | PLC 通信接口，提供读写操作 |
| `ConnectionConfig` | 连接配置接口，定义连接参数 |
| `PlcResolve` | 数据解析器，用于批量读取时转换数据 |

## 项目结构

```
plc4x-spring-boot-starter/
├── src/main/java/com/yunyoucloud/plc4x/
│   ├── MainApplication.java          # Spring Boot 启动类
│   ├── client/                       # PLC 客户端
│   │   ├── PLC.java                  # PLC 读写接口
│   │   └── PlcManager.java           # 连接管理器
│   ├── config/                       # 配置类
│   │   ├── PlcConfig.java            # PLC 配置属性
│   │   ├── S7PlcConfig.java          # S7 特定配置
│   │   ├── ModbusPlcConfig.java      # Modbus 特定配置
│   │   ├── OpcuaPlcConfig.java       # OPC UA 特定配置
│   │   └── connection/               # 连接配置
│   ├── exception/                    # 异常定义
│   │   ├── PlcReadExpection.java
│   │   ├── PlcWriteExpection.java
│   │   └── PlcCommExpection.java
│   ├── serializer/                   # 序列化器
│   └── utils/                        # 工具类
plc4x-spring-boot-starter-api/        # API 模块
```

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 许可证开源。

## 致谢

- [Apache PLC4X](https://plc4x.apache.org/) - 工业协议通信库
- [Spring Boot](https://spring.io/projects/spring-boot) - Spring Boot 框架

## 连接生命周期与断线恢复

`PLC` 实现了 `AutoCloseable`。短期连接请使用 `try (PLC plc = PlcManager.getPlc(config)) { ... }`；长期复用连接必须在设备移除、配置替换及应用停止时调用 `PlcManager.close(plc)`。关闭是永久操作，关闭后请创建新的 `PLC`，旧对象不会被后台读写重新打开。`destroyStaleConnection()` 仅用于故障失效，保留后续重连能力。

连接失败后每个冷却周期最多尝试一次，默认冷却 5000 毫秒。单点及批量读写默认最多等待 10000 毫秒；创建原始驱动连接及协议握手共用 10000 毫秒建连预算；一次完整关闭及兜底重试默认最多等待 5000 毫秒。自定义 `ConnectionConfig` 可覆盖 `reconnectIntervalMillis()`、`requestTimeoutMillis()`、`connectTimeoutMillis()` 和 `closeTimeoutMillis()`：三个超时值必须大于零，重连间隔不能为负。这些方法是 Java 扩展接口，不是 YAML 属性。各阶段分别计时，不包含获取客户端锁的等待时间。受管协议请求也受驱动自身超时限制，以先到的截止时间为准。

建连及关闭共用最多 8 个守护线程和 64 个排队位置，空闲线程在 1 秒后回收。生命周期操作超时会移除排队任务或中断正在执行的驱动；底层若忽略中断，在其实际退出之前禁止同一客户端再次建连或重叠关闭。`close()` 会报告失败并保留所有权，调用方应保留对象、稍后重试，不能通过持续创建客户端绕开等待。Java 中断不能强制终止任意第三方阻塞代码。

优先使用 `PlcManager.getPlc(config, false)` 先取得客户端，再由读写或 `checkAndReconnectConnection()` 建连。若仍使用立即连接的入口，且首次连接失败后清理也未完成，会抛出运行时异常 `PlcInitializationException`；通过 `getClient()` 取得仍拥有资源的客户端，保存并重试其 `close()`。异常的 cause 保留建连失败原因，suppressed 中保留清理失败原因。首次连接失败但已清理成功时，继续报告原有建连异常。

同一个 `PLC` 的读写、替换及关闭串行执行。同一应用类加载器中的客户端共享驱动注册表，设备连接独立持有。先接管原始连接再显式 `connect()`，握手失败也会调用 `close()`。读写请求失败会取消对应 Future，仅在确认断链时清理连接：暴露的真实传输通道已关闭或不活跃，或者异常链包含 `ClosedChannelException`、`EOFException`、`SocketException`。服务端 `BadInternalError` 等协议错误、普通请求异常、超时、取消和线程中断本身均不触发关闭，异常仍向调用方报告，后续读写复用同一连接。`SocketTimeoutException` 也不作为断链依据。S7H 暴露的 `EmbeddedChannel` 是虚拟通道，不用它推断真实 TCP 状态；无法确认断链时保留连接。仅超时的静默故障可能持续报错，不能保证自动重建。设备移除和应用退出时仍须显式关闭客户端。

确认断链后沿用资源清理和重连冷却。正常关闭抛异常后，先通过 `ChannelExposingConnection` 取得的通道执行传输层关闭（最多等待 2 秒），触发 PLC4X 的通道关闭回调，再重试一次驱动关闭以完成剩余清理。两次关闭失败或完整清理等待超时后仍保留连接引用，后续清理成功前禁止创建替代连接。显式调用 `PLC.close()` / `PlcManager.close()` 在清理最终失败时抛出 `IllegalStateException`，调用方应保留客户端并重试关闭。通道关闭不等于驱动全部资源都已释放，因此不能仅凭通道关闭成功就覆盖旧引用。未暴露通道的连接仅重试驱动关闭。

标准 S7、Modbus TCP 和 OPC UA 驱动均接入针对 **PLC4X 0.13.1** 的受管适配器，保留上游报文编码及安全配置，没有新增 Maven 依赖。只替换标准驱动的精确类型，自定义驱动及其子类保持原实现。三种标准协议均使用受管 TCP 工厂：建连失败、中断或超时后，尚未交付的通道也会关闭并回收专属线程组。已关闭通道的重复清理先释放协议资源，再清空连接引用，避免向已退出的事件循环重复发送关闭事件。

| 范围 | 修复行为 |
|------|----------|
| 三种协议共有 | 每连接最多 64 个未完成事务、64 个 API 响应和 64 个报文等待器；容量耗尽明确失败。成功、错误、取消、超时及断线都会释放事务槽和关联等待器。异步后续事务继承父请求的截止时间与取消状态，外层聚合请求取消会传递到全部协议子请求。排队时间计入超时，过期或已取消的排队写请求不会在恢复后补发。已经开始发送的请求无法撤回。 |
| Modbus TCP | 修复超时或错误后事务槽不释放、后续请求永久排队的问题。晚到响应仍按事务编号匹配，不会用作其他请求的结果。 |
| S7 | 物理通道关闭后从受管 TCP 工厂移除引用，并停止其专属事件循环组；永久关闭时拒绝内部重连。保留单端点及双端点 S7H 地址。解码执行器使用有界队列，读写解码异常保留原始原因并及时结束 Future。拆包聚合取消时同步取消子请求，中断或预算耗尽后停止继续提交。变长 STRING/WSTRING 写入按设备最大容量编码，不再按旧值长度截断，超容量值在写入阶段前拒绝。定长 `STRING(n)/WSTRING(n)` 按地址声明的容量检查；STRING 按 UTF-8 字节数、WSTRING 按 UTF-16 代码单元计算，数组逐项检查。原生订阅异步衔接报文响应，不再阻塞解码线程等待；订阅和取消订阅均受超时及取消控制。 |
| OPC UA | 每个订阅最多一个在途 Publish，停止时取消该请求；删除订阅失败或无响应时立即停止本地发布任务和业务回调。取消订阅返回真实 Future，远端删除失败通过 Future 报告；失败后再次取消会重试，执行中及成功结果复用。远端报告订阅已不存在也视为清理成功。修复消费者注销递归及重复注册计数。TCP 断开、显式关闭时取消本连接的订阅、保活任务和请求。所有入站分片先进行解密、验签和连续序列校验，包括已超时请求的晚到响应；晚到响应不会重新完成已取消的请求，也不会导致后续正常读取持续报 Lost sequence。修正非对称验签判断及加密长度换算。原生订阅不会跨新连接自动恢复，需要重新创建。 |

由于 PLC4X 0.13.1 未公开必要清理接口，适配器集中使用少量私有字段/方法访问：S7 事务与解码执行器、OPC UA 事务/安全通道/认证步骤/订阅回调，以及 SPI 响应注册表/计时器和 TCP 工厂配置。包级加密入口通过 `org.apache.plc4x.java.opcua.context.ManagedOpcuaEncryption` 适配；使用普通 classpath 加载，未验证 JPMS 模块路径。字段不存在时明确失败；升级 PLC4X 必须重新验证，不能假设适配器跨版本兼容。报文计时器和本地订阅调度使用共享守护执行器，取消后移除任务，空闲线程可回收。不会关闭未知归属的线程组，也不会停止 PLC4X 的全局共享 OPC UA 保活执行器。受管 OPC UA 连接复用上游握手、认证及令牌编码，使用独立管理的共享守护调度器续期，不启动原生非守护保活线程；每连接最多一个续期请求，停止后晚到响应不能重启调度，空闲线程可回收。`S7ConnectionConfig.port` 现在明确写入 URL，`ip` 字段仅填写主机名或 IP。

单点读取的空响应、缺失状态和非 `OK` 状态现在抛出 `PlcReadExpection`，不再返回 `null` 后导致基本类型拆箱异常。批量读取拒绝重复标签，按请求字段解析，全部解析完成后再更新样本；空批量读取直接返回。

回归测试不依赖现场设备：

```shell
mvn "-Dtest=com.yunyoucloud.plc4x.client.*Test" -Dsurefire.failIfNoSpecifiedTests=false test
```

现场直接内存耗尽还需要断电恢复长时间验证：固定设备和请求并发，循环断线/恢复，观察 Netty 存活线程、连接数及直接内存是否在预热后稳定。测试环境可启用 `-Dio.netty.leakDetection.level=paranoid` 并采集分配栈（有性能开销）；泄漏检测没有报错不能单独证明不存在泄漏。该测试类覆盖连接所有权和异常清理，不代表已证明 PLC4X/Netty 或上层业务的全部分配路径没有泄漏。
连接复用和重连不依赖底层 `plcConnection.isConnected()`，该返回值在现场驱动中不能可靠反映断线。`PLC.isConnected()` 检查本地生命周期及可见的真实通道状态，不保证设备实时在线。`connect()` 正常返回后标记可用；请求错误不改变该标记。确认断链后清理旧连接并重连；访问前已发现通道关闭时直接尝试替换，清理或重连失败后进入冷却。仅超时不能判断设备是否断开。

## 写入返回值与确认范围

`PLC.write(...)`、`writeBoolean/writeInteger/...`、`IPLCSerializable.write(bean)` 及序列化器的类型写入方法现在返回 `boolean`。例如：

```java
boolean confirmed = serializer.writeInteger("ns=2;i=1001", 123);
```

- `true`：已等待写响应，本次请求的每个字段均得到 PLC4X `PlcResponseCode.OK`。
- `false`：未取得全部字段的成功确认，包括远端错误、缺失字段状态、空响应以及无写入字段。批量部分成功不会回滚，`false` 不能理解为所有值均未写入。
- 超时、断线、线程中断等继续抛出异常，仅确认断链时清理连接。请求可能已经到达设备，只是响应没有到达客户端，因此异常时写入结果可能未知。

根据 [OPC UA Write 规范](https://reference.opcfoundation.org/specs/OPC-10000-4/5.11.4.1)，普通 `Good` 表示服务器报告数据源写入成功；仅写入中间系统、不能确认数据源更新时，应报告 `Good_CompletesAsynchronously`。已核对本项目 PLC4X 0.13.1 的 `OpcuaProtocolLogic.writeResponse` / `mapOpcStatusCode`：普通 `Good` 映射为 `OK`，异步完成状态映射为非 `OK`，因此不会返回 `true`。

`true` 仍然是对服务器写响应的确认，没有额外读回验证，也不等于 PLC 程序已扫描并接受命令或设备动作已完成。若要确认控制逻辑已处理，应由 PLC 提供命令编号和确认编号/应答位。读回相同节点只能证明当时读取到相同值，网关可能返回缓存，不能独立证明物理设备已执行。

返回类型由 `void` 变为 `boolean` 是二进制不兼容的 API 修改。原调用源码可以继续忽略返回值，但上层应用及依赖本包的模块必须重新编译；自定义 `IPLCSerializable` 实现也要更新方法签名。单点远端错误现在返回 `false`，调用方需要检查结果，不能仅依赖捕获异常。

写入和连接回归测试：

```shell
mvn -Dtest=PlcWriteResultTest,PlcLifecycleTest,PlcConnectionCleanerTest -Dsurefire.failIfNoSpecifiedTests=false test
```
