//! The threads this library owns, so that no wait for an activation holds a
//! BEAM scheduler: a poller per worker, and the starvation watch.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crossbeam::channel::{unbounded, Receiver, Sender};
use flz_core::schema::{ActivationOutcome, ActivationResult, WorkError};
use flz_core_ffi::veneer::notice::Notice;
use flz_core_ffi::veneer::{self, shape, Polled};
use flz_core_ffi::{FLZ_POLL_TIMEOUT, FLZ_STARVATION_OBSERVED};
use rustler::env::OwnedEnv;
use rustler::{Atom, Encoder, LocalPid, Resource, ResourceArc};

use crate::terms::Sink;
use crate::{atoms, Core};

/// How long one native wait lasts before the thread looks at its stop flag.
const WAIT_MS: u64 = 250;

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

pub fn poller(core: ResourceArc<Core>, worker: LocalPid) -> ResourceArc<Thread> {
    let (thread, waiting) = Thread::new();
    let own = thread.clone();
    std::thread::Builder::new()
        .name("flz-poller".to_string())
        .spawn(move || poll_for(core, own, waiting, worker))
        .expect("a poller thread starts");
    thread
}

fn poll_for(
    core: ResourceArc<Core>,
    thread: ResourceArc<Thread>,
    waiting: Receiver<()>,
    worker: LocalPid,
) {
    let mut env = OwnedEnv::new();
    while !thread.stopped() {
        let Ok(inside) = core.enter() else { return };
        let ctx = inside.ctx();
        let act = match veneer::poll(ctx, WAIT_MS) {
            Ok(Polled::Activation(act)) => act,
            Ok(Polled::Timeout) => continue,
            Ok(Polled::ShuttingDown) => {
                let _ = env.send_and_clear(&worker, |e| atoms::flz_shutting_down().encode(e));
                return;
            }
            Err(refusal) => {
                let document = veneer::document(refusal.into_message());
                let _ =
                    env.send_and_clear(&worker, |e| (atoms::flz_poll_failed(), document).encode(e));
                return;
            }
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
            if let Some(token) = thread.take_token() {
                let _ =
                    veneer::complete(ctx, &failure(token, "the worker it was handed to is gone"));
            }
            return;
        }
        // Out of the context while the worker runs the activation.
        drop(inside);
        if waiting.recv().is_err() {
            return;
        }
    }
}

pub fn starvation_watch(core: ResourceArc<Core>, owner: LocalPid) -> ResourceArc<Thread> {
    let (thread, _waiting) = Thread::new();
    let own = thread.clone();
    std::thread::Builder::new()
        .name("flz-starvation".to_string())
        .spawn(move || {
            let mut env = OwnedEnv::new();
            while !own.stopped() {
                let Ok(inside) = core.enter() else { return };
                match veneer::wait_starvation(inside.ctx(), WAIT_MS) {
                    Ok(FLZ_STARVATION_OBSERVED) => {
                        if env
                            .send_and_clear(&owner, |e| atoms::flz_starved().encode(e))
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(FLZ_POLL_TIMEOUT) => {}
                    _ => return,
                }
            }
        })
        .expect("the starvation watch starts");
    thread
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
