# Multi-Agent AIOps

这是一个可运行的学习版，使用 Python 实现。

它模拟了一次完整的 AIOps 故障处置流程：

```text
MonitorAgent（异常检测）→ RCAAgent（根因分析）
→ HealAgent（自愈方案）→ ChangeAgent（变更审批）
```

## 运行方式

```bash
cd python
pip install -r requirements.txt

# 方式一：命令行 Demo
python -m core.orchestrator

# 方式二：FastAPI
python -m uvicorn api.main:app --reload --port 8000
```

启动后访问：

- API 文档：http://localhost:8000/docs
- 触发一次故障处理：`POST /api/v1/incidents/trigger`
- 异常检测 Demo：`GET /api/v1/anomaly/demo`
- 服务拓扑：`GET /api/v1/topology`

## 目录结构

```text
python/
├── models/       # 事件模型 + 时序异常检测
├── core/         # 事件总线 + 知识图谱 + 编排器
├── agents/       # 4 个 Agent
├── api/          # FastAPI 入口
├── config/       # 全局配置
└── tests/        # 测试
```

```bash
git init
git add .
git commit -m "feat: multi-agent AIOps Python replica"
git branch -M main
git remote add origin https://github.com/你的用户名/multi-agent-aiops-replica.git
git push -u origin main
```
