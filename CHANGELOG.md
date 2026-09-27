# Changelog

本项目的所有重要变更都会记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.0.2] - 2026-09-27

### Fixed

- 修复 `ActorHandle` 被 clone 后，任意一个副本 drop 会意外取消整个 actor 的 bug。
  现在 `ActorHandle` drop 不再触发取消，只有显式调用 `.stop()` 才停止 actor。
  所有 handle 都被 drop 后，actor 通过 `cmd_rx` 关闭自然退出。

### Changed

- `ActorHandle` 不再实现 `Drop`。停止 actor 请显式调用 `.stop()`。
- `ManagerCmd::SpawnActor` 添加了 `Option<oneshot::Sender<usize>>` 参数，用于立刻接收返回的 id。
- 所有的`listen` 要求的闭包取消了bool 返回值，监听取消必须依靠返回的 CancelToken

## [0.0.1] - 2026-09-26

### Added

- 首个版本。
- `Actor` trait、`ActorHandle`、`ActorContext`。
- `ActorManager` 管理器。
- `CancelToken` / `CancelHandle`。
