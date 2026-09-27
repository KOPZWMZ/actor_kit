use std::time::Duration;

// tests/basic.rs
use actor_lite::{Actor, ActorContext};
struct Counter {
    count: u32,
}

enum Cmd {
    Inc,
}

impl Actor for Counter {
    type Cmd = Cmd;
    type Event = ();
    type State = u32;
    type Progress = ();

    fn init_state_and_progress(&self) -> (u32, ()) {
        (self.count, ())
    }

    fn run(self, mut ctx: ActorContext<Self>) -> impl Future<Output = ()> + Send + 'static {
        async move {
            let mut count = self.count;
            let cancel = ctx.cancel_clone();
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    Some(Cmd::Inc) = ctx.wait_for_cmd() => {
                        count += 1;
                        ctx.update_state(count);
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn counter_handles_inc() {
    let handle = Counter { count: 0 }.spawn();
    handle.send_cmd(Cmd::Inc).await.unwrap();
    handle.state_listen_async(|state| async move {
        assert_eq!(state, 1);
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.stop();
}
