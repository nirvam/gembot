# Gembot

Gembot 是一个将 `gemini-cli` 代理能力接入即时通讯平台（如飞书）的中间件服务。它利用 ACP (Agent Client Protocol) 协议与底层 `gemini-cli` 进程通信，管理并发会话，并处理平台特定的消息交互与展示。

## 核心架构时序图

以下时序图展示了 Gembot 处理一条消息的完整生命周期：

```mermaid
sequenceDiagram
    autonumber
    actor User as 用户 (User)
    participant Feishu as 飞书 (Feishu)
    participant Adapter as FeishuAdapter
    participant Store as SQLite Store
    participant Manager as Core Manager (Worker)
    participant Bridge as ACP Bridge
    participant Agent as gemini-cli (Subprocess)

    User->>Feishu: 发送消息 / 回复消息
    Feishu->>Adapter: P2MessageReceiveV1 (WebSocket)
    
    %% 幂等与会话识别
    Adapter->>Store: IsProcessed(msgID)
    Store-->>Adapter: false
    Adapter->>Store: MarkProcessed(msgID)
    
    note over Adapter: 解析 RootId 或 MessageId 作为 TopicID
    
    %% 发送占位并异步启动流
    Adapter->>Feishu: 发送占位消息 "⏳ 正在思考..."
    Feishu-->>Adapter: 返回 replyMsgID
    
    Adapter->>Manager: HandleMessage(topicID, message, ...)
    note over Manager: 根据 TopicID 进行 Hash 路由<br/>分发给专属 Worker，避免并发冲突
    
    Manager->>Store: GetSessionRecord(topicID)
    Store-->>Manager: 记录状态 (nil 或已存在)
    
    %% 会话生命周期管理
    alt 无记录 (新话题)
        Manager->>Bridge: NewSession()
        Bridge->>Agent: ACP: new_session
        Agent-->>Bridge: 返回 sessionID
        Bridge-->>Manager: sessionID
    else 有记录但内存无活跃状态 (跨进程或清理后)
        Manager->>Bridge: LoadSession(sessionID)
        Bridge->>Agent: ACP: load_session
        Agent-->>Bridge: 恢复上下文
    end
    Manager->>Store: SaveSession(topicID, sessionID)
    
    %% 发送指令并建立流
    Manager->>Bridge: SendMessage(sessionID, prompt)
    Bridge->>Agent: ACP: prompt
    Bridge-->>Manager: 返回 StreamEvent Channel (updateCh)
    
    %% 异步流式更新机制
    par 消费与更新
        loop Event Stream
            Agent-->>Bridge: ACP SessionUpdate (AgentMessage/ToolCall)
            Bridge-->>Adapter: StreamEvent (Text/Log)
        end
    and 定时/节流 Patch
        loop 聚合与上屏 (Ticker 1s)
            Adapter->>Feishu: Patch(replyMsgID, content, logs)
        end
    end
    
    %% 结束
    Agent-->>Bridge: 结束标志
    Bridge-->>Manager: Channel Close
    Manager-->>Adapter: 结束 Patch
    Adapter->>Feishu: 最终状态上屏
```

## 工程规范与架构亮点

* **Hash Routing**: 基于 TopicID 的一致性哈希路由确保同一会话的消息串行处理。
* **Fail-Safe 机制**: `Manager` 层具备 `recover` 与异常捕获机制，保证飞书侧的消息不会永久悬挂为“正在思考...”。
* **Idempotency**: 通过 SQLite 的 `processed_messages` 表防止 WebSocket 重传导致的消息重复执行。
* **接口隔离**: 抽象 `core.Adapter` 与 `acp.Bridge`，核心逻辑与具体 IM 平台和协议解耦。
