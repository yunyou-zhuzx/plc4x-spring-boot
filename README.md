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

连接失败后不再持锁休眠重试三次，而是每个冷却周期最多尝试一次。默认冷却 5000 毫秒；单点及批量读写的 Future 等待默认最多 10000 毫秒。自定义 `ConnectionConfig` 可覆盖 `reconnectIntervalMillis()` 和 `requestTimeoutMillis()`。这两个方法目前是 Java 扩展接口，不是 YAML 属性。等待上限不包括锁等待、建连握手和驱动关闭耗时。

同一个 `PLC` 的读写、替换及关闭串行执行。每个客户端复用一个驱动管理器。先取得原始连接再显式 `connect()`，握手失败也会调用 `close()`；断线连接无论在线标志如何均会关闭。读写超时、异常完成、提交异常或线程中断会取消 Future 并释放连接。Future 取消本身不能替代底层通道释放。正常关闭抛异常后，先通过 `ChannelExposingConnection` 取得的通道执行传输层关闭（最多等待 2 秒），触发 PLC4X 的通道关闭回调，再重试一次驱动关闭以完成剩余清理。两次驱动关闭都失败时仍保留连接引用，后续清理成功前禁止创建替代连接。显式调用 `PLC.close()` / `PlcManager.close()` 在清理最终失败时抛出 `IllegalStateException`，调用方应保留客户端并重试关闭，不能丢弃对象或直接新建替代连接。通道关闭不等于驱动全部资源都已释放，因此不能仅凭通道关闭成功就覆盖旧引用。未暴露通道的连接仅重试驱动关闭。传输层等待上限不限制驱动 `close()` 自身的耗时。清理代码不创建后台线程池，也不直接关闭未知归属的事件循环组。调用方应复用客户端，不能在每个失败请求中重新创建客户端绕过冷却。

回归测试不依赖现场设备：

```shell
mvn -Dtest=PlcLifecycleTest,PlcConnectionCleanerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

现场直接内存耗尽还需要断电恢复长时间验证：固定设备和请求并发，循环断线/恢复，观察 Netty 存活线程、连接数及直接内存是否在预热后稳定。测试环境可启用 `-Dio.netty.leakDetection.level=paranoid` 并采集分配栈（有性能开销）；泄漏检测没有报错不能单独证明不存在泄漏。该测试类覆盖连接所有权和异常清理，不代表已证明 PLC4X/Netty 或上层业务的全部分配路径没有泄漏。
连接复用和重连不依赖底层 `plcConnection.isConnected()`，该返回值在现场驱动中不能可靠反映断线。`PLC.isConnected()` 仅表示本地连接已建立且尚未被通信故障标记失效，不保证设备实时在线。`connect()` 正常返回后标记可用；实际读写超时或异常时标记失效、清理并进入重连冷却。静默断线会在下一次实际通信时被发现。
