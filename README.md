# MoviePilot-Plugins（pronii 自用插件市场）

自用 MoviePilot v3 插件市场。

## 插件列表

### PopularSubscribe 热门媒体订阅（v3.1.0）

基于 [thsrite/MoviePilot-Plugins](https://github.com/thsrite/MoviePilot-Plugins) 的 PopularSubscribe v3.0.0 本地魔改：

- **数据源改造**：不再依赖 MoviePilot 官方 Server 的订阅统计接口，改为直接拉取
  **爱奇艺 / 腾讯视频 / 芒果TV** 三大平台片库「最新」电视剧榜单
- 可勾选启用任一平台组合，按平台独立 cron 定时拉取
- 自动排重：已订阅 / 已入库 / 历史处理过的不再重复订阅
- 订阅历史记录展示来源平台

## 安装

在 MoviePilot v3 的插件市场中添加本仓库地址即可。

## 致谢

原插件：thsrite（MIT/开源社区惯例），原版功能（MP Server 订阅统计热门订阅）依然保留，平台订阅为新增能力。
