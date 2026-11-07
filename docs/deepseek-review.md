# DeepSeek review 记录

来源：用户指定 jihuayu/skills 的 deepseek-subagent，独立只读会话，主代理负责真实运行与最终整合。

| 发现 | 处理与回归 |
| --- | --- |
| B1 stale expectedVersion 发布新草稿 | publish必须提供选中的最新draft版本；current_version_id仅为附加前置条件；integration回归409 |
| B2 EOF被补造DONE | 缺少DONE触发incomplete_stream和partial；不补造完成标记；gateway回归 |
| B3 waitDrain无限等候 | 按整体deadline限时等待，超时销毁响应；模拟不读取客户端回归 |
| B4 unknown多次更正重复计费 | 第一个actual最终化；同值幂等，不同值409；ledger总额回归 |
| B5 alias/error/attempt元数据缺失 | 安全元数据增量持久化及finalize；模型筛选和两次尝试真实PG回归 |
| 预算键丢失 | 已知period缺少预算hash触发恢复；不归零放行 |
| 主节点身份变化 | 客户端run_id检查+Lua执行时run_id原子检查，重启和真实副本提升演练 |
| 结算连接池死锁 | PG提交后先释放client，再执行缓存结算；100并发100笔账本 |
| 更正聚合重复/错误分类 | 同一请求的账本补偿先净额化，取最终计量，不重复token |

最终结论：**ACCEPT（blocker-closure，静态审阅）**。独立 DeepSeek reviewer 对冻结实现提交 `0cbd74bb9cb271219f045e2eb707140b290ce9d6` 确认 B1–B5 全部关闭，未发现变更区域中的其它阻塞缺陷。后续 CI 引用修正与交付记录不改变该实现提交的业务代码。

Reviewer保留的两项确认已由主代理核实：events.ts 对每个请求先净额化账本再取最终计量分类（events.test.ts 覆盖更正不重复）；cancelBeforeDispatch 与 settle 均在请求行锁之前获取相同项目 advisory xact lock。

Reviewer只读、未执行测试；真实运行证据由主代理在验证报告提供。性能目标未达标继续保留为已知限制，不能宣称通过生产SLO。

## 首轮性能优化独立review

同一用户指定skill，模型 `auto/deepseek-v4.1-flash`。DeepSeek实现worker完成项目FIFO队列与6项测试后释放实现所有权，主代理整合SQL合并、批次消费与真实运行；新的独立只读会话审阅 `d1f0600..994e7d4`，最终给出 **ACCEPT**，无阻塞项。首次403已在用户要求重试后恢复；review的API超时和输出截断分别续接，最终取得完整结论，没有将中断状态视为通过。

Reviewer静态核对首次结算四类写入的原子性和参数映射、意图冲突后的新快照读取、generation读取、项目/请求锁顺序、关闭时排空已接受工作、元数据重试与批次失败回退。没有发现费用、授权或恢复方面的阻塞回归。该结论不包含独立执行测试或性能目标认证。

Reviewer还指出非阻塞的诊断回归：finalize仅传未持久化attempt，导致全量attempt均已增量写入时将请求error_code清空。主代理在 `6251405f587984d54dfd2647a7b8e08ae120d648` 将错误推导使用的完整attempt列表与需写入的行分开。真实HTTP断流回归先复现null错误码，再修复；真实PG集成同时覆盖失败码保留与成功备用清除先前错误。完整65通过/2专用跳过套件及类型检查再次通过。该提交也在测试用例边界关闭/排空并重新初始化Billing，避免前一故障用例的自动恢复任务干扰下一用例；生产billing代码未随此补丁改变。

剩余非阻塞边界：冷缓存项目查找仍多一次索引查询；毒消息会先消耗一次失败批次事务再逐条重试；outbox批次ack依赖当前lease契约，未来加入reaper时需补围栏；元数据仍为尽力持久化。独立后台池、增量投影与历史规模性能由#21继续跟踪，固定资源及SSE指标由#22跟踪。

同一独立reviewer对 `994e7d4..6251405` 的修复补丁完成静态关闭审阅，结论同为 **ACCEPT**：诊断问题关闭，测试fixture隔离改动可接受，无阻塞项。报告没有执行测试或重新测量性能；保留的非阻塞事项包括fixture重复初始化成本、UUID隔离下保留的requestCache，以及未来调用者必须保持已持久化attempt不再变更的约定。最终业务代码冻结于 `6251405f587984d54dfd2647a7b8e08ae120d648`，后续只补文档和证据。
