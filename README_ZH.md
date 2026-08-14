# Proxy2API

Proxy2API 是一个基于 sing-box 的代理池管理工具。

目标是把大量上游节点统一成稳定的本地 HTTP/SOCKS5 代理入口，同时支持按节点独立端口访问。

## 当前能力

- 运行模式：`pool`、`multi-port`、`hybrid`。
- 实际构建的上游协议：`vmess`、`vless`、`trojan`、`ss/shadowsocks`、`hysteria2/hy2`、`socks5/socks`、`http/https`、`anytls`、`tuic`。
- 节点来源：
  - `shared.yaml` 的 `nodes`
  - `nodes_file`（每行一个 URI）
  - `subscriptions`（支持 Base64/纯文本/Clash YAML 解析）
- 自动健康检查、失败熔断和黑名单恢复。
- 拨号失败自动重试：节点拨号失败时自动切换到另一个健康节点重试（可配置次数）。
- 端口稳定（multi-port/hybrid）：每个节点按 URI 稳定标识（忽略名称与参数顺序），订阅刷新或重启后保持同一本地端口（持久化到各项目目录的 `node_ports.json`）。
- 粘性代理：可选的独立端口，按来源 IP 把客户端固定绑定到同一上游节点，保持出口 IP 稳定，与轮询的 pool 入口共存（仅 pool/hybrid 模式）。
- Web 管理面板 + API：
  - 节点状态/探测/导出
  - **手动拉黑/解封节点**
  - 项目运行设置（`external_ip`、`probe`、`skip_cert_verify`、监听/节点池/粘性配置）
  - 节点配置增删改查 + 重载
  - 共享订阅定义管理；订阅状态、刷新时间和周期按项目独立保存
  - 系统设置页内的**实时日志控制台**
- 新增可配置 DNS 解析器（对 VMess 域名节点非常关键）。
- **可配置日志轮转**，支持大小限制、备份数量和压缩。

## 多项目隔离

管理面板使用一个全局监听端口。节点定义和订阅定义独立保存在 `shared.yaml`，并被所有项目共用；从任一项目编辑共享源都会写入同一文件。每个项目拥有独立的 sing-box 实例、订阅管理器、节点缓存、端口映射、运行状态数据库和代理池状态。项目 A 的订阅刷新、探测、黑名单、粘性绑定、重载和停止不会修改项目 B。

首次升级时无需手工迁移：程序会从 `-config` 指定的现有配置提取共享源，并把原运行设置迁移为 `default` 项目：

```text
config.yaml                         # 启动入口及旧配置迁移源（兼容现有部署）
shared.yaml                         # 独立的共享节点和订阅定义
projects.yaml                       # 全局管理配置和项目清单
projects/<project-id>/project.yaml  # 项目运行配置
nodes.txt                           # 共享源的兼容节点缓存
projects/<project-id>/nodes.txt
projects/<project-id>/node_ports.json
projects/<project-id>/.subscription-cache.json
projects/<project-id>/.proxy2api-state.db
```

全局配置包括管理面板的 `enabled`、`listen`、`password`、日志输出和轮转策略，以及项目名称、启用状态和自动启动策略。共享源配置包括 `nodes` / `nodes_file`、`subscriptions` 和订阅启用状态。代理模式、入口端口和认证、节点池、粘性入口、`probe`、订阅刷新周期、外部 IP 与证书策略均跟随项目。

管理面板仅在新建/编辑项目时显示项目 ID，其他页面使用项目名称。各项目的探测目标、探测间隔、探测超时和并发数统一在“节点配置”页面管理；自动订阅更新开关和更新间隔统一在“订阅配置”页面管理。

订阅 URL 和节点定义只维护一份；每个项目独立保存订阅缓存、订阅最后刷新/下次刷新时间、节点健康状态和黑名单计时器。仅在管理面板中切换项目不会抓取订阅或触发探测。修改共享节点/订阅定义后，运行中的项目会重载共享目录；各项目仍使用自己的状态数据库和项目刷新周期。显式重启项目时会在恢复本项目状态后重新验证节点，但探测结果绝不会跨项目复用。

项目 ID 只能包含小写字母、数字、`-` 和 `_`。所有代理入口及项目内部流量 API 端口都会统一检查；冲突时只拒绝当前项目操作，不会调整或停止其他项目。通过管理面板删除任意项目（包括默认项目）时，可以选择仅将其移出清单并保留数据，或同时删除标准项目目录；默认保留本地文件。`shared.yaml` 位于项目目录之外，两种删除方式都不会删除共享节点和订阅。项目清单允许为空；空清单中新建的第一个项目会自动成为新的默认项目。

## 快速开始

### 1）准备配置

```bash
cp config.example.yaml config.yaml
cp nodes.example nodes.txt
```

编辑 `config.yaml`，并配置节点来源（`nodes.txt` / `subscriptions` / `nodes`）。

> 为什么要 `touch nodes.txt`？如果你用文件级挂载（如 `-v ./data/nodes.txt:/etc/Proxy2API/nodes.txt`）而宿主机上该文件不存在，Docker 会在宿主机上创建一个名为 `nodes.txt` 的**目录**并挂载进去，容器内就出现"本应是文件却是目录"的坑。预先创建文件（或直接挂载**目录** `./data:/etc/Proxy2API`，首启动会自动生成文件）可避免。若已踩坑：`rm -rf ./data/nodes.txt && touch ./data/nodes.txt` 后重启。

### 2）启动

Docker：

```bash
docker compose up -d
```

本地运行：

```bash
go run ./cmd/Proxy2API -config config.yaml
```

## 最小配置示例（Pool）

```yaml
mode: pool

listener:
  address: 0.0.0.0
  port: 2323
  username: user
  password: pass

pool:
  mode: sequential    # sequential / random / balance / latency
  failure_threshold: 3
  blacklist_duration: 24h
  retry_enabled: true # 拨号失败时切换到另一节点重试
  retry_attempts: 3   # 每个请求的最大拨号次数

management:
  enabled: true
  listen: 0.0.0.0:9091
  password: ""

probe:
  target: http://cp.cloudflare.com/generate_204
  interval: 5m
  timeout: 1m50s
  concurrency: 32

dns:
  server: 223.5.5.5
  port: 53
  strategy: prefer_ipv4

nodes_file: nodes.txt
```

服务启动时不会自动探测节点。节点初始显示为“未测试”，需在 WebUI 手动探测；启用周期探测后，首次自动探测在 `probe_interval` 到期时执行。

## 粘性代理（可选，仅 Pool/Hybrid 模式）

开启后会额外监听一个独立端口（默认 `listener.port + 1`，即 `2324`），与原 `2323` 端口共存。粘性入口可通过监控面板指定出口节点；未指定时默认选择最低延迟节点，再按**来源 IP** 保持绑定。原主入口始终按 `pool.mode` 调度，不受粘性入口的指定节点影响。监听地址与认证复用 `listener` 配置。

```yaml
sticky:
  enabled: true
  port: 2324    # 留空或 0 则默认为 listener.port + 1
  fixed_node: "" # 可选；留空时选择最低延迟节点
```

## DNS 配置说明

`dns` 会同时影响 sing-box DNS 客户端和 VMess 域名拨号解析：

```yaml
dns:
  server: 223.5.5.5
  fallback_servers:    # 备用 DNS 服务器（主 DNS 解析失败时使用）
    - 8.8.8.8
    - 1.1.1.1
  port: 53
  strategy: prefer_ipv4
```

`strategy` 可选值：

- `as_is`
- `prefer_ipv4`
- `prefer_ipv6`
- `ipv4_only`
- `ipv6_only`

如果日志中出现 `lookup <domain>: empty result`，请优先检查该 DNS 配置是否可达且策略合理。

## 运行模式

- `pool`：所有节点共享一个本地 HTTP/SOCKS5 入口。
- `multi-port`：每个节点一个独立本地 HTTP/SOCKS5 端口。
- `hybrid`：同时启用 pool + multi-port。

## 节点来源行为

- 配置了 `subscriptions` 时：
  - 启动时只读取 `.subscription-cache.json` 或 `nodes_file` 的本地缓存，不联网更新，也不改写节点文件
  - 通过 WebUI 的“更新”按钮或刷新 API 手动抓取订阅；只有显式开启 `subscription_refresh.enabled` 后才会定时刷新
  - `nodes_file` 作为手动/定时订阅更新后的节点写入路径
  - 可通过 `subscription_refresh.fetch_concurrency` 调整订阅抓取并发数（默认 16，最大 32）
- `nodes`（内联节点）只要存在就会参与运行。
- **多来源节点合并**：当同时配置 `nodes` 和 `subscriptions` 时：
  - 内联节点（`shared.yaml` 中的 `nodes`）和订阅节点会合并使用
  - 订阅更新时会保留内联节点，不会覆盖
  - 节点顺序：内联节点在前，订阅节点在后
  - 各节点的来源标识（inline/subscription）会在管理界面中显示
- **端口稳定**（multi-port/hybrid）：节点按 URI 稳定标识（忽略名称与参数顺序），订阅改名或重排都保持同一本地端口；分配结果保存到项目目录的 `node_ports.json`，重启后自动恢复。删除该文件可强制重新分配。

## 协议支持注意事项

运行时真正支持的协议：

- `vmess`
- `vless`
- `trojan`
- `ss` / `shadowsocks`
  - 支持 SIP002：`ss://base64(method:password)@server:port#name`
  - 支持旧格式：`ss://base64(method:password@server:port)#name`
- `hysteria2` / `hy2`
- `socks5` / `socks5h` / `socks`
- `http` / `https`
- `anytls`
- `tuic`

订阅解析阶段可能识别到更多 URI 前缀（兼容输入），但不在上述列表中的协议会在构建阶段被跳过。

## 管理 API（核心）

- `POST /api/auth`
- `GET|POST /api/projects`
- `GET|PATCH|DELETE /api/projects/{project_id}`（删除时可传 `?delete_data=true` 同时删除标准项目目录；省略或传 `false` 时保留；共享配置始终保留）
- `POST /api/projects/{project_id}/start|stop|reload`
- `/api/projects/{project_id}/...`（项目级状态、探测、黑名单、设置、流量和日志接口）
- `GET|POST|PUT|DELETE /api/nodes/config[...]`（共享节点定义；任一项目的写操作都落到 `shared.yaml`）
- `GET|POST|PUT|PATCH|DELETE /api/subscriptions`（共享订阅定义；`PATCH` 用于开启/关闭订阅）
- `GET|PUT /api/system/settings`（全局管理面板和日志配置）
- `GET|PUT /api/settings`（当前项目运行配置的兼容 API）
- `GET /api/nodes`
- `GET /api/nodes/online`（仅返回当前在线节点的端口、延迟、IP 和地域等信息）
- `POST /api/nodes/{tag}/probe`
- `POST /api/nodes/{tag}/release`
- `POST /api/nodes/{tag}/blacklist`（可选 JSON：`{"duration":"1h"}`）
- `POST /api/nodes/probe-all`（SSE）
- `GET|PUT|DELETE /api/sticky/fixed-node`（指定粘性入口出口；清空后恢复最低延迟选择）
- `GET /api/export`
- `GET|PUT /api/subscription/config`
- `GET|POST /api/subscription/status|refresh`
- `POST /api/reload`

未带项目前缀的旧接口继续操作 `default_project`，用于兼容已有脚本；没有项目时返回 HTTP 503，项目清单和系统设置接口仍然可用。

`management.password` 为空时，Web/API 不要求登录。

Python 调用示例：

```python
from urllib.parse import quote

import requests

base = "http://127.0.0.1:9091"
session = requests.Session()

# 配置了 management.password 时需要先登录；未配置时删除此行即可。
session.post(f"{base}/api/auth", json={"password": "your-password"}).raise_for_status()

nodes = session.get(f"{base}/api/nodes/online").json()
tag = nodes["nodes"][0]["tag"]

# 项目级状态必须带项目 ID；A、B 可分别拉黑同一共享节点。
session.post(
    f"{base}/api/projects/a/nodes/{quote(tag, safe='')}/blacklist",
    json={"duration": "1h"},
).raise_for_status()
session.post(f"{base}/api/projects/b/nodes/{quote(tag, safe='')}/blacklist", json={"duration": "30m"}).raise_for_status()
session.post(f"{base}/api/projects/a/nodes/{quote(tag, safe='')}/release").raise_for_status()

# 指定粘性入口出口；DELETE 恢复最低延迟选择。
session.put(f"{base}/api/sticky/fixed-node", json={"tag": tag}).raise_for_status()
session.delete(f"{base}/api/sticky/fixed-node").raise_for_status()
```

## 重要运行说明

- 重载（`/api/reload` 或订阅刷新）会中断现有连接。
- Settings API 会把项目设置写回对应的 `projects/<project-id>/project.yaml`；部分设置需要重载后才能完全生效。
- 省略项默认值可在 `internal/config/config.go` 中查看。
- 日志轮转通过 `log` 配置段设置；当 `output: file` 时，日志同时写入控制台和文件，并自动轮转。

## 常见问题

### 配置持久化问题

**问题描述**：在 Docker 环境中通过 WebUI 修改配置后，重启或重建容器时配置被重置。

**快速诊断**：
```bash
# 检查 data 目录结构和权限
ls -la data/
[ -f data/config.yaml ]  || echo "缺少 data/config.yaml"
[ -d data/config.yaml ]   && echo "异常：data/config.yaml 是目录（见快速开始说明）"
[ -d data/nodes.txt ]    && echo "异常：data/nodes.txt 是目录（见快速开始说明）"
```

**常见原因和解决方案**：

1. **文件权限问题**：
   ```bash
   # 修复权限
   chown -R $(id -u):$(id -g) data
   chmod 755 data
   chmod 644 data/config.yaml data/nodes.txt
   ```

2. **卷映射错误**：
   - 确保 `docker-compose.yml` 中使用 `./data:/etc/Proxy2API`
   - 不要使用绝对路径或错误的目录

3. **启动时未传递 UID/GID**：
   ```bash
   # 正确的启动方式
   UID=$(id -u) GID=$(id -g) docker-compose up -d
   ```

**验证配置是否保存**：
```bash
# 查看文件修改时间
ls -lh data/config.yaml data/nodes.txt

# 查看容器日志，确认保存成功
docker-compose logs -f | grep "Saved"
```

**详细故障排查**：参见 [docs/troubleshooting-persistence.md](docs/troubleshooting-persistence.md)

### Docker 权限问题

**问题描述**：使用 `docker-compose.yml` 映射配置目录时，可能遇到 "permission denied" 或 "cannot write to /etc/Proxy2API" 等权限错误。

**原因分析**：容器以非 root 用户运行（docker-compose.yml 中指定 `user: "${UID:-10001}:${GID:-10001}"`），但宿主机挂载目录的所有权可能不匹配。

**解决方案**：

1. **使用 docker compose 挂载目录（推荐）**：
   ```bash
   mkdir -p data logs
   sudo chown -R $(id -u):$(id -g) data logs
   docker compose up -d
   ```
   直接挂载整个 `./data` 目录，首启动会自动生成 `config.yaml` 和 `nodes.txt` 文件。

2. **预先创建配置文件**（备选，适用于文件级挂载）：
   ```bash
   mkdir -p data
   cp config.example.yaml data/config.yaml
   touch data/nodes.txt
   chown -R $(id -u):$(id -g) data
   docker compose up -d
   ```

**docker run 命令方式**：
```bash
mkdir -p data logs
chmod -R u+w data logs
docker run --user $(id -u):$(id -g) \
  -v $(pwd)/data:/etc/Proxy2API \
  -v $(pwd)/logs:/app/logs \
  --network host \
  ghcr.io/CY-Curry30/Proxy2API:latest
```

### 其他常见问题

- **"配置文件未找到"**：确保挂载目录中存在 `config.yaml` 文件
- **"无法绑定端口"**：检查端口是否被其他服务占用
- **"所有节点健康检查失败"**：验证代理 URI 格式正确，且上游服务器可达
- **"代理之前正常使用，突然失效"**：检查节点是否被加入黑名单（连续失败 3 次后触发，默认持续 24 小时）
  - **解决方案 1**：通过 WebUI 释放 - 点击节点旁边的"释放"按钮
  - **解决方案 2**：通过 API 释放 - `POST http://localhost:9091/api/nodes/{tag}/release`
  - **解决方案 3**：在 `config.yaml` 中降低黑名单持续时间：
    ```yaml
    pool:
      blacklist_duration: 1h  # 从默认的 24h 改为 1h
    ```
  - 查看黑名单事件日志：`docker compose logs | grep "BLACKLISTED"`

## 更新日志

详见 [CHANGELOG.md](CHANGELOG.md)。

## 开发验证

```bash
go test ./...
```

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=CY-Curry30/Proxy2API&type=Date)](https://star-history.com/#CY-Curry30/Proxy2API&Date)

## 致谢

本项目基于 [sing-box](https://github.com/SagerNet/sing-box) 构建 —— 底层所有协议实现、传输层与拨号逻辑都由 sing-box 提供。特别感谢 SagerNet 团队及所有贡献者的卓越工作。

## 许可证

MIT License
