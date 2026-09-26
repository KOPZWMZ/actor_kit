use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Notify, broadcast, mpsc, watch};
use tracing::debug;
/// Actor 模型的核心 trait。
///
/// 实现此 trait 来定义一个 actor 的行为。每个 actor 有四个关联类型：
///
/// | 类型 | 说明 |
/// |------|------|
/// | [`Cmd`](Actor::Cmd) | 命令枚举，外部通过它向 actor 发送指令 |
/// | [`Event`](Actor::Event) | 事件类型，actor 对外广播的异步消息 |
/// | [`State`](Actor::State) | 低频状态，变化不频繁，可随时读取最新值 |
/// | [`Progress`](Actor::Progress) | 高频进度，适合进度条、实时指标等场景 |
///
/// # 生命周期
///
/// 1. 调用 [`Actor::spawn`] 启动 actor，得到 [`ActorHandle`]
/// 2. 通过 [`ActorHandle`] 发送命令、读取状态/进度、订阅事件
/// 3. 调用 [`ActorHandle::stop`] 或 drop handle 来停止 actor
///
/// # 示例
///
/// ```no_run
/// # use actor_kit::{Actor, ActorContext};
/// struct Counter {
///     target: u32,
/// }
///
/// enum CounterCmd {
///     Reset,
/// }
///
/// impl Actor for Counter {
///     type Cmd = CounterCmd;
///     type Event = u32;
///     type State = u32;
///     type Progress = f32;
///
///     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
///         (0, 0.0)
///     }
///
///     fn run(self, context: ActorContext<Self>) -> impl std::future::Future<Output = ()> + Send + 'static {
///         let mut context = context;
///         async move {
///             let mut count = 0u32;
///             loop {
///                 if let Some(cmd) = context.check_cmd() {
///                     match cmd {
///                         CounterCmd::Reset => {
///                             count = 0;
///                             context.update_state(count);
///                             context.update_progress(0.0);
///                         }
///                     }
///                 }
///                 if count >= self.target {
///                     break;
///                 }
///                 count += 1;
///                 context.update_state(count);
///                 context.update_progress(count as f32 / self.target as f32);
///                 context.send_event(count);
///             }
///         }
///     }
/// }
/// ```
pub trait Actor: Sized + Send + 'static {
    /// actor 命令枚举
    ///
    /// 例如：
    /// ```
    /// enum MyCmd {
    ///     Start,
    ///     Pause,
    ///     //...
    /// }
    /// ```
    type Cmd: Send + 'static;
    /// actor 里的事件结构体，需要实现`Clone`
    type Event: Clone + Send + 'static;
    /// actor 里的低频状态，如果不需要，用 `type State = ();`。
    type State: Clone + Send + Sync + 'static;
    /// actor 里的高频状态，如果没有高频进度时，用 `type Progress = ();`。
    type Progress: Clone + Send + Sync + 'static;

    /// actor 的命令通道容量
    const CMD_CAPACITY: usize = 16;
    /// actor 的事件通道容量
    const EVENT_CAPACITY: usize = 16;
    /// 创建 actor 的通道。
    /// ActorContext 是 actor 的后台端，ActorHandle 是 actor 的控制端。
    fn channel(
        init_state: Self::State,
        init_progress: Self::Progress,
    ) -> (ActorContext<Self>, ActorHandle<Self>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(Self::CMD_CAPACITY);
        let (state_tx, state_rx) = watch::channel(init_state);
        let (event_tx, _) = broadcast::channel(Self::EVENT_CAPACITY);
        let (progress_tx, progress_rx) = watch::channel(init_progress);
        let cancel = CancelToken::new();
        let handle_cancel = CancelHandle { cancel };
        let context_cancel = handle_cancel.clone();

        let context = ActorContext {
            cmd_rx,
            event_tx: event_tx.clone(),
            state_tx,
            progress_tx,
            cancel: context_cancel,
        };
        let handle = ActorHandle {
            cmd_tx,
            event_tx,
            state_rx,
            progress_rx,
            cancel: handle_cancel,
        };
        (context, handle)
    }

    fn init_state_and_progress(&self) -> (Self::State, Self::Progress);
    /// 启动 actor。
    ///
    /// 返回 actor 的控制端。
    ///
    ///  外部通过 [`ActorHandle`] 的方法发送命令、监听状态/进度、订阅事件。
    ///
    fn spawn(self) -> ActorHandle<Self> {
        let (init_state, init_progress) = self.init_state_and_progress();
        let (context, handle) = Self::channel(init_state, init_progress);
        tokio::spawn(self.run(context));
        handle
    }
    /// actor的主要循环
    ///
    /// 运行 actor 的主要逻辑。
    ///
    /// # 注意
    ///
    /// 接收后台端并自行决定循环和发送事件，更新状态的流程
    ///
    /// # 示例
    ///
    /// ```no_run
    ///  # use actor_kit::{Actor, ActorHandle, ActorContext};
    /// struct MyActor;
    /// impl Actor for MyActor {
    ///     type Cmd = ();
    ///     type Event = ();
    ///     type State = ();
    ///     type Progress = ();
    ///     fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
    ///         ((), ())
    ///     }
    ///     fn run(self, context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
    ///         let mut context = context;
    ///     async move {
    ///     
    ///         loop {
    ///         if let Some(cmd) = context.check_cmd() {
    ///             // ...处理命令
    ///             }
    ///          //...主要逻辑
    ///
    ///          //发送事件和进度，更新状态
    ///          context.send_event(());
    ///          context.update_progress(());
    ///          context.update_state(());
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    fn run(self, context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static;
}
/// actor 的控制端。
///
/// 由 [`Actor::channel`] 创建，通常由外部控制方持有。
/// 用于发送命令、读取状态/进度、订阅事件，以及注册异步监听。
/// # 示例
///
/// ```no_run
/// # use actor_kit::{Actor, ActorHandle, ActorContext};
/// # struct MyActor;
/// # impl Actor for MyActor {
/// #    type Cmd = ();
/// #    type Event = ();
/// #    type State = ();
/// #    type Progress = ();
/// #    fn init_state_and_progress(&self) -> (Self::State, Self::Progress) {
/// #        ((), ())
/// #    }
/// #    fn run(self, context: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
/// #         async {}
/// #     }
/// # }
/// # async fn example() {
/// let (context, handle) = MyActor::channel( (), ());
///
/// handle.send_cmd(()).await.unwrap();
/// let state = handle.get_state();
///
/// let listener = handle.state_listen_async(|s| async move { true });
/// listener.cancel();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ActorHandle<A: Actor> {
    cmd_tx: mpsc::Sender<A::Cmd>,
    state_rx: watch::Receiver<A::State>,
    event_tx: broadcast::Sender<A::Event>,
    progress_rx: watch::Receiver<A::Progress>,
    cancel: CancelHandle,
}

impl<A: Actor> ActorHandle<A> {
    /// 向 actor 发送一条命令。
    pub async fn send_cmd(&self, cmd: A::Cmd) -> Result<(), mpsc::error::SendError<A::Cmd>> {
        self.cmd_tx.send(cmd).await
    }
    /// 获取 actor 的最新状态。
    pub fn get_state(&self) -> A::State {
        self.state_rx.borrow().to_owned()
    }
    /// 异步监听状态变化。
    ///
    /// 每次状态变化时会调用 `op`，传入最新状态。
    /// `op` 返回 `true` 继续监听，返回 `false` 停止。
    ///
    /// 返回的 [`ListenerHandle`] 被 drop 或调用 `cancel()` 时停止监听。

    pub fn state_listen_async<F, Fut>(&self, mut op: F) -> CancelHandle
    where
        F: FnMut(A::State) -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send,
    {
        let token = CancelToken::new();
        let wait_token = token.clone();
        let mut rx = self.state_rx.clone();
        let listen = async move {
            loop {
                tokio::select! {
                    _ = wait_token.cancelled() => break,
                    result = rx.changed() => {
                        match result {
                        Ok(())=>{
                            let state = rx.borrow_and_update().to_owned();
                             if !op(state).await{
                                break;
                            }
                        }
                        Err(_) => {break}}
                    }
                }
            }
        };
        tokio::spawn(listen);
        CancelHandle { cancel: token }
    }
    /// 获取 actor 的最新进度。
    pub fn get_progress(&self) -> A::Progress {
        self.progress_rx.borrow().to_owned()
    }
    pub fn progress_listen_async<F, Fut>(&self, mut op: F) -> CancelHandle
    where
        F: FnMut(A::Progress) -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send,
    {
        let token = CancelToken::new();
        let wait_token = token.clone();
        let mut rx = self.progress_rx.clone();
        let listen = async move {
            loop {
                tokio::select! {
                    _ = wait_token.cancelled() => break,
                    result = rx.changed() => {
                        match result {
                        Ok(())=>{
                            let progress = rx.borrow_and_update().to_owned();
                             if !op(progress).await{
                                break;
                            }
                        }
                        Err(_) => {break}}
                    }// 幂等、同步、永不失败
                }
            }
        };
        tokio::spawn(listen);
        CancelHandle { cancel: token }
    }

    pub fn get_events_rx(&self) -> broadcast::Receiver<A::Event> {
        self.event_tx.subscribe()
    }
    pub fn events_listen_async<F, Fut>(&self, mut op: F) -> CancelHandle
    where
        F: FnMut(A::Event) -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send,
    {
        let mut rx = self.event_tx.subscribe();
        let token = CancelToken::new();
        let wait_token = token.clone();
        let listen = async move {
            loop {
                tokio::select! {
                    _ = wait_token.cancelled() => break,
                    result = rx.recv() => {
                        match result {
                            Ok(e) => { if  !op(e).await { break; } }
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                debug!("事件监听落后 {n} 条");
                                break;
                            // 继续，或 break，看语义
                        }
                    }}
                }
            }
        };
        tokio::spawn(listen);
        CancelHandle { cancel: token }
    }
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

/// actor 的后台端。
///
/// 由 [`Actor::channel`] 创建，通常由后台任务持有。
/// 用于接收命令、更新状态/进度、广播事件。
pub struct ActorContext<A: Actor> {
    cmd_rx: mpsc::Receiver<A::Cmd>,
    state_tx: watch::Sender<A::State>,
    event_tx: broadcast::Sender<A::Event>,
    progress_tx: watch::Sender<A::Progress>,
    cancel: CancelHandle,
}
impl<A: Actor> ActorContext<A> {
    /// 获取额外的事件发送端。
    pub fn get_events_clone(&self) -> broadcast::Sender<A::Event> {
        self.event_tx.clone()
    }
    /// 发送事件。
    pub fn send_event(&self, event: A::Event) {
        let _ = self.event_tx.send(event);
    }
    /// 更新状态。
    pub fn update_state(&self, state: A::State) {
        let _ = self.state_tx.send(state);
    }
    /// 更新进度。
    pub fn update_progress(&self, progress: A::Progress) {
        let _ = self.progress_tx.send(progress);
    }
    /// 阻塞等待命令。
    pub fn wait_for_cmd(&mut self) -> impl Future<Output = Option<A::Cmd>> {
        self.cmd_rx.recv()
    }
    /// 非阻塞检查命令。
    pub fn check_cmd(&mut self) -> Option<A::Cmd> {
        self.cmd_rx.try_recv().ok()
    }

    pub fn cancel_clone(&self) -> CancelHandle {
        self.cancel.clone()
    }
}
/// 取消信号。
///
/// 可在多个任务间 clone 共享。任意一份调用 [`CancelToken::cancel`]，
/// 所有通过 [`CancelToken::cancelled`] 等待的任务都会收到通知。
/// `cancel` 是幂等的。
/// ...
/// # 注意
///
/// `CancelToken` 是 `Clone` 的，clone 出来的所有副本共享同一个取消信号。
/// 任意一份 `cancel()` 都会让所有副本的 `cancelled()` 返回。
#[derive(Clone, Debug)]
struct CancelToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }
    /// 检查取消信号。
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
    /// 等待取消信号。
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
    /// 发送取消信号。
    pub fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }
}
/// 监听句柄。
///
/// 由 `state_listen_async` / `progress_listen_async` / `events_listen_async` 返回。
/// 调用 [`CancelHandle::cancel`] 或直接 drop 都会停止对应的监听任务。
#[derive(Debug, Clone)]
pub struct CancelHandle {
    cancel: CancelToken,
}

impl CancelHandle {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
}

impl Drop for CancelHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
