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
