# actor-lite

基于 tokio 的轻量 actor 抽象。

## 安装

```toml
[dependencies]
actor-lite = "0.0.1"
```

 这个 crate 不规定 actor 怎么运行，只提供一套通道组合和监听接口。
 你为后台任务实现 [`Actor`] trait，就能拿到：

- 一个 [`ActorHandle`]，用于发送命令、读取状态/进度、订阅事件
- 一个 [`ActorContext`]，用于在后台任务里接收命令、更新状态/进度、广播事件

 actor 的 `spawn` 逻辑由你自己决定，这个 crate 只负责"控制"和"观察"这一层。

# 状态

 早期开发中，API 可能随时变化。

# 模块

 | 模块 | 说明 |
 |------|------|
 | 根模块（`actor`） | 提供 [`Actor`]、[`ActorHandle`]、[`ActorContext`]、[`CancelHandle`] 核心抽象 |
 | [`manager`] | 提供 [`ActorManager`](manager::ActorManager)，用于管理一组同类型子 actor 并聚合其状态/进度/事件 |

# 你得到什么

 实现 [`Actor`] 后，调用 `Actor::channel` 会得到两端：

 | 端 | 谁持有 | 用途 |
 |----|--------|------|
 | [`ActorContext`] | 后台任务 | 收命令、更新状态/进度、广播事件 |
 | [`ActorHandle`] | 外部控制方 | 发命令、读状态/进度、订阅事件、异步监听 |

# 监听

 [`ActorHandle`] 上的 `state_listen_async` / `progress_listen_async` / `events_listen_async`
 均返回 [`CancelHandle`]，主动 `cancel()` 或直接 drop 都会停止对应的监听任务。

# 快速开始

## 定义一个 actor

 ```no_run
 use actor_lite::{Actor, ActorContext};
 use std::future::Future;

 struct MyActor;

 impl Actor for MyActor {
     type Cmd = ();
     type Event = ();
     type State = ();
     type Progress = ();

     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
         ((), ())
     }

     fn run(self, context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
         let mut context = context;
         async move {
             loop {
                 if let Some(cmd) = context.check_cmd() {
                     // ...处理命令
                 }
                 // ...主要逻辑

                 // 发送事件、更新进度和状态
                 context.send_event(());
                 context.update_progress(());
                 context.update_state(());
             }
         }
     }
 }
 ```

## 使用 manager 管理多个子 actor

 当需要统一管理多个同类型 actor 时，使用 [`manager::ActorManager`]：

 ```no_run
 use actor_lite::manager::{ActorManager, ManagerCmd, ManagerEvent};
 use actor_lite::{Actor, ActorContext};
 use std::future::Future;
  struct MyActor;
  impl Actor for MyActor {
      type Cmd = ();
      type Event = ();
      type State = ();
      type Progress = ();
      fn init_state_and_progress(&self) -> (Self::State, Self::Progress) { ((), ()) }
      fn run(self, context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static { async {} }
  }
  async fn example() {
 // ActorManager 本身也是 Actor，直接 spawn 即可
 let handle = ActorManager::<MyActor>::new().spawn();

 let (id_tx,id_rx) = tokio::sync::oneshot::channel();
 handle.send_cmd(ManagerCmd::SpawnActor{actor:MyActor,id_tx:Some(id_tx)}).await.unwrap();
 let id = id_rx.await.unwrap();

 // 读取所有子 actor 的聚合状态
 let state = handle.get_state();
 println!("共 {} 个子 actor", state.len());

 // 订阅事件流
 let mut events = handle.get_events_rx();
 while let Ok(event) = events.recv().await {
     match event {
         ManagerEvent::Spawned { id } => println!("actor {id} 已启动"),
         ManagerEvent::Stopped  { id } => println!("actor {id} 已停止"),
         ManagerEvent::ChildEvent { id, event } => { /* ... */ }
     }
 }
 # }
 ```
