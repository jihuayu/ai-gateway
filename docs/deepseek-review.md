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

最终候选关闭review正在针对冻结提交执行，完成后补充确切提交与结论。review未运行测试的部分以主代理附带的真实运行证据为依据。
