//! The threads this library owns, so that no wait for an activation holds a
//! BEAM scheduler: a poller per worker, and the pool governor's supervisor.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam::channel::{unbounded, Receiver, Sender};
use flz_core::poll_pool::{self, Governor};
use flz_core::schema::{Activation, ActivationOutcome, ActivationResult, WorkError};
use flz_core_ffi::veneer::notice::Notice;
use flz_core_ffi::veneer::{self, shape, Polled, Refusal};
use rustler::env::OwnedEnv;
use rustler::{Atom, Encoder, LocalPid, Resource, ResourceArc};

use crate::terms::Sink;
use crate::{atoms, Core};

/// The pool governor of one pool process.
pub struct Pool(pub Arc<Governor>);

#[rustler::resource_impl]
impl Resource for Pool {}

/// A governor over `default_base` whose supervisor asks `owner` for each
/// worker as `{:flz_grow, grown}`, the base ones first. An owner gone stops it.
/// A nonzero `retire_ms` is a test's short retire time.
pub fn pool(default_base: usize, retire_ms: u64, owner: LocalPid) -> ResourceArc<Pool> {
    let mut governor = Governor::from_env(default_base);
    if retire_ms > 0 {
        let retire = Duration::from_millis(retire_ms);
        governor = Governor::new(governor.base, governor.ceiling, retire);
    }
    let stops = Arc::clone(&governor);
    governor.start(flz_core::poll_core::global(), move |grown: bool| {
        let mut env = OwnedEnv::new();
        env.send_and_clear(&owner, |e| (atoms::flz_grow(), grown).encode(e))
            .map_err(|_| {
                stops.stop();
                std::io::Error::other("the pool process is gone")
            })
    });
    ResourceArc::new(Pool(governor))
}

/// A native thread, as the BEAM holds it.
pub struct Thread {
    stopped: AtomicBool,
    /// Lets a poller poll again: its worker completed, or it is told to stop.
    turn: Sender<()>,
    /// The activation a poller handed out and its worker has not answered.
    outstanding: Mutex<Option<u64>>,
}

#[rustler::resource_impl]
impl Resource for Thread {}

impl Thread {
    fn new() -> (ResourceArc<Thread>, Receiver<()>) {
        let (turn, waiting) = unbounded();
        let thread = ResourceArc::new(Thread {
            stopped: AtomicBool::new(false),
            turn,
            outstanding: Mutex::new(None),
        });
        (thread, waiting)
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn hand_out(&self, token: Option<u64>) {
        *self.outstanding.lock().unwrap_or_else(|e| e.into_inner()) = token;
    }

    /// The activation handed out and not yet answered, taken so that only
    /// one path answers it.
    pub fn take_token(&self) -> Option<u64> {
        self.outstanding
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    pub fn resume(&self) {
        self.hand_out(None);
        let _ = self.turn.send(());
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let _ = self.turn.send(());
    }
}

/// A failed answer to `token`, for an activation no worker can answer.
pub fn failure(token: u64, message: &str) -> ActivationResult {
    ActivationResult {
        token,
        outcome: ActivationOutcome::Failed(WorkError {
            message: message.to_string(),
            non_retryable: false,
        }),
    }
}

/// A poller in a seat `pool` reserved; it gives the seat back however it ends.
pub fn poller(
    core: ResourceArc<Core>,
    pool: ResourceArc<Pool>,
    worker: LocalPid,
    grown: bool,
) -> ResourceArc<Thread> {
    let (thread, waiting) = Thread::new();
    let own = thread.clone();
    std::thread::Builder::new()
        .name("flz-poller".to_string())
        .spawn(move || {
            let _seat = pool.0.seat();
            poll_for(core, own, waiting, worker, &pool.0, grown);
        })
        .expect("a poller thread starts");
    thread
}

/// One poll turn, in the milliseconds the veneer's poll takes.
const POLL_MS: u64 = poll_pool::POLL_TIMEOUT.as_millis() as u64;

fn poll_for(
    core: ResourceArc<Core>,
    thread: ResourceArc<Thread>,
    waiting: Receiver<()>,
    worker: LocalPid,
    pool: &Governor,
    grown: bool,
) {
    let mut env = OwnedEnv::new();
    let mut idle = Duration::ZERO;
    while !thread.stopped() {
        // A freed context ends the pool: stopped before the seat is given
        // back, so the governor does not try to replace this thread.
        let Ok(inside) = core.enter() else {
            pool.stop();
            return;
        };
        let ctx = inside.ctx();
        let act = match veneer::poll(ctx, POLL_MS) {
            Ok(Polled::Activation(act)) => act,
            Ok(Polled::Timeout) if !pool.idle_turn(grown, &mut idle) => continue,
            last => return tell_last(&mut env, &worker, last),
        };
        let token = act.token;
        let mapped = env.run(|e| shape::activation(&Sink(e), act).map(|term| env.save(term)));
        let saved = match mapped {
            Ok(saved) => saved,
            Err(f) => {
                let _ = veneer::complete(ctx, &failure(token, &f.0));
                continue;
            }
        };
        thread.hand_out(Some(token));
        let sent = env.send_and_clear(&worker, |e| {
            (atoms::flz_activation(), thread.clone(), saved.load(e)).encode(e)
        });
        if sent.is_err() {
            // The worker never saw it: handed back as if never polled.
            let _ = thread.take_token().map(|token| veneer::abandon(ctx, token));
            return;
        }
        // Out of the context while the worker runs the activation.
        drop(inside);
        if waiting.recv().is_err() {
            return;
        }
        idle = Duration::ZERO;
    }
}

/// A poller's last word to its worker: `:stop` for a grown one gone idle, the
/// core shutting down, or the refusal that ended its poll.
fn tell_last(env: &mut OwnedEnv, worker: &LocalPid, last: Result<Polled<Activation>, Refusal>) {
    let _ = env.send_and_clear(worker, |e| match last {
        Ok(Polled::ShuttingDown) => atoms::flz_shutting_down().encode(e),
        Err(refusal) => {
            let document = veneer::document(refusal.into_message());
            (atoms::flz_poll_failed(), document).encode(e)
        }
        _ => atoms::stop().encode(e),
    });
}

/// One core notice to `sink`; `false` when it is gone.
pub fn send_notice(sink: LocalPid, notice: &Notice) -> bool {
    let mut env = OwnedEnv::new();
    env.send_and_clear(&sink, |e| {
        let level = Atom::from_str(e, notice.level.name()).expect("a level name is an atom");
        (
            atoms::flz_notice(),
            level,
            notice.target,
            notice.text.as_str(),
        )
            .encode(e)
    })
    .is_ok()
}
