//! The BEAM as a veneer host over `flz_core`, through the `veneer` layer every
//! other non-Rust SDK binds. Elixir terms are the language: atoms for the
//! vocabulary, binaries for payloads, `{key, value}` tuples for metadata and
//! maps with snake_case atom keys for records.
//!
//! No call that parks for an activation runs on a BEAM scheduler: each worker
//! is paired with a thread this library owns (`threads`), and every other call
//! that can wait runs on a dirty-IO scheduler.

mod terms;
mod threads;

use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use flz_core_ffi::veneer::{self, shape, CtxPtr, EffectStart, Refusal};
use flz_core_ffi::FlzContext;
use rustler::types::binary::Binary;
use rustler::{Atom, Encoder, Env, Error, LocalPid, NifResult, Resource, ResourceArc, Term};
use terms::{bin, Fail, Sink, Source};
use threads::Thread;

mod atoms {
    rustler::atoms! {
        ok, kept, run, unrecorded, flz_activation, flz_starved, flz_shutting_down, flz_poll_failed,
        event_time, processing_time
    }
}

/// One core context. Freed once, by `context_free`, after every call inside
/// it has left.
pub struct Core {
    ctx: AtomicPtr<FlzContext>,
    freed: AtomicBool,
    /// Calls inside the context now: a NIF on any process, or one wait of a
    /// thread this library owns.
    inside: AtomicUsize,
}

/// How long `context_free` waits for the calls inside the context to leave.
const CALLS_LEAVE_WITHIN: Duration = Duration::from_secs(5);

#[rustler::resource_impl]
impl Resource for Core {}

/// One call inside the context, counted until it is dropped.
pub struct Entered<'a>(&'a Core);

impl Entered<'_> {
    pub fn ctx(&self) -> CtxPtr {
        CtxPtr::of(self.0.ctx.load(Ordering::SeqCst))
    }
}

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        self.0.inside.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Core {
    /// Enter the context, or refuse once it is freed. Counting in before
    /// reading `freed`, against `context_free` setting it before reading the
    /// count, means one of the two always sees the other.
    pub fn enter(&self) -> NifResult<Entered<'_>> {
        self.inside.fetch_add(1, Ordering::SeqCst);
        let entered = Entered(self);
        if self.freed.load(Ordering::SeqCst) {
            return Err(refusal("the core context is freed".to_string()));
        }
        Ok(entered)
    }
}

/// A refusal as `{:error, document}`: `message` when it is already an error
/// document, else one naming it.
fn refusal(message: String) -> Error {
    Error::Term(Box::new(veneer::document(message)))
}

/// The context whose runner or local run is live, by address; 0 for none.
/// The core's activation queue is process-wide, so a second would hand its
/// activations to the first one's workers. Held until the context is freed,
/// which is after its last poller stopped.
static LIVE: Mutex<usize> = Mutex::new(0);

impl Core {
    /// Begin a runner or a local run on this context, holding the process's
    /// core for it; a refused `begin` gives the core back.
    fn begin(&self, begin: impl FnOnce(CtxPtr) -> Result<(), Refusal>) -> NifResult<Atom> {
        let me = self.ctx.load(Ordering::SeqCst) as usize;
        let took = {
            let mut live = LIVE.lock().unwrap_or_else(|e| e.into_inner());
            if *live != 0 && *live != me {
                return Err(refusal(flz_core::Code::CoreInUse.with([]).json()));
            }
            let took = *live == 0;
            *live = me;
            took
        };
        let begun = self
            .enter()
            .and_then(|inside| begin(inside.ctx()).map_err(refused));
        // Only a claim this call took is given back: a refused second begin
        // leaves the context's live run its claim.
        if begun.is_err() && took {
            self.release();
        }
        begun.map(|()| atoms::ok())
    }

    fn release(&self) {
        let me = self.ctx.load(Ordering::SeqCst) as usize;
        let mut live = LIVE.lock().unwrap_or_else(|e| e.into_inner());
        if *live == me {
            *live = 0;
        }
    }
}

fn refused(r: Refusal) -> Error {
    refusal(r.into_message())
}

fn failed(f: Fail) -> Error {
    refusal(f.0)
}

fn c(what: &str, value: &str) -> NifResult<std::ffi::CString> {
    veneer::c_string(what, value).map_err(refused)
}

// --- the context --------------------------------------------------------------

#[rustler::nif]
fn schema_version() -> u32 {
    flz_core_ffi::flz_core_schema_version()
}

#[rustler::nif]
fn context_new(shim_schema_version: u32) -> NifResult<ResourceArc<Core>> {
    let ctx = veneer::context_new(shim_schema_version).map_err(refused)?;
    Ok(ResourceArc::new(Core {
        ctx: AtomicPtr::new(ctx),
        freed: AtomicBool::new(false),
        inside: AtomicUsize::new(0),
    }))
}

/// Free the context once no call is inside it. A call that does not leave in
/// time keeps the context alive: a leak is recoverable, a use after free is
/// not.
#[rustler::nif(schedule = "DirtyIo")]
fn context_free(core: ResourceArc<Core>) -> Atom {
    if core.freed.swap(true, Ordering::SeqCst) {
        return atoms::ok();
    }
    // The claim outlives the calls still inside: a poller in its wait would
    // otherwise take the next run's first activation.
    let deadline = Instant::now() + CALLS_LEAVE_WITHIN;
    while core.inside.load(Ordering::SeqCst) > 0 {
        if Instant::now() >= deadline {
            core.release();
            return atoms::ok();
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    veneer::context_free(CtxPtr::of(core.ctx.load(Ordering::SeqCst)));
    core.release();
    atoms::ok()
}

#[rustler::nif]
fn check_class<'a>(env: Env<'a>, core: ResourceArc<Core>, json: Binary) -> NifResult<Term<'a>> {
    let derived = veneer::check_class(core.enter()?.ctx(), json.as_slice()).map_err(refused)?;
    Ok(bin(env, &derived))
}

#[rustler::nif]
fn declare_class(core: ResourceArc<Core>, json: Binary) -> NifResult<Atom> {
    veneer::declare_class(core.enter()?.ctx(), json.as_slice()).map_err(refused)?;
    Ok(atoms::ok())
}

/// Connect with the settings the SDK read, never the process environment.
#[rustler::nif(schedule = "DirtyIo")]
fn start(core: ResourceArc<Core>, namespace: String, settings: Binary) -> NifResult<Atom> {
    let namespace = c("namespace", &namespace)?;
    core.begin(|ctx| veneer::start_with(ctx, &namespace, settings.as_slice()))
}

#[rustler::nif]
fn shutdown(core: ResourceArc<Core>) -> NifResult<Atom> {
    veneer::shutdown(core.enter()?.ctx());
    Ok(atoms::ok())
}

#[rustler::nif]
fn relays(core: ResourceArc<Core>) -> NifResult<Vec<String>> {
    veneer::relays(core.enter()?.ctx()).map_err(refused)
}

// --- dispatch -------------------------------------------------------------------

/// A thread that polls for `worker` and sends it `{:flz_activation, thread,
/// act}`; it polls again only after the worker's `complete` names it.
#[rustler::nif]
fn poller(core: ResourceArc<Core>, worker: LocalPid) -> NifResult<ResourceArc<Thread>> {
    core.enter()?;
    Ok(threads::poller(core, worker))
}

/// A thread that sends `owner` `:flz_starved` each time the core observes
/// starvation.
#[rustler::nif]
fn starvation_watch(core: ResourceArc<Core>, owner: LocalPid) -> NifResult<ResourceArc<Thread>> {
    core.enter()?;
    Ok(threads::starvation_watch(core, owner))
}

/// Stop a thread once it is back from the wait it is in; an activation its
/// worker holds is still the worker's to answer.
#[rustler::nif]
fn stop_thread(thread: ResourceArc<Thread>) -> Atom {
    thread.stop();
    atoms::ok()
}

/// Stop a thread whose worker is gone. An activation that worker never
/// answered is answered as a failure, so the core's retry ladder takes it
/// rather than waiting on it.
#[rustler::nif(schedule = "DirtyIo")]
fn release_thread(core: ResourceArc<Core>, thread: ResourceArc<Thread>) -> Atom {
    if let (Some(token), Ok(entered)) = (thread.take_token(), core.enter()) {
        let _ = veneer::complete(
            entered.ctx(),
            &threads::failure(token, "the worker that held it exited"),
        );
    }
    thread.stop();
    atoms::ok()
}

/// Answer the activation `thread` handed out, then let it poll again. A
/// result that does not map still answers the activation, as a failure.
#[rustler::nif(schedule = "DirtyIo")]
fn complete<'a>(
    env: Env<'a>,
    core: ResourceArc<Core>,
    thread: ResourceArc<Thread>,
    result: Term<'a>,
) -> NifResult<Atom> {
    let answered = match shape::result(&Source(env), &result) {
        // Taken first: an activation `release_thread` already failed back is
        // not answered a second time.
        Ok(result) => match thread.take_token() {
            Some(_) => veneer::complete(core.enter()?.ctx(), &result).map_err(refused),
            None => Ok(()),
        },
        Err(f) => {
            if let Some(token) = thread.take_token() {
                let _ = veneer::complete(core.enter()?.ctx(), &threads::failure(token, &f.0));
            }
            Err(failed(f))
        }
    };
    thread.resume();
    answered.map(|()| atoms::ok())
}

// --- calls a tasklet makes from its live activation ------------------------------

#[allow(clippy::too_many_arguments)]
#[rustler::nif]
fn spawn_request(
    core: ResourceArc<Core>,
    token: u64,
    factory_id: String,
    id: String,
    payload: Binary,
    if_running: String,
    replay: bool,
    offset: Option<u64>,
) -> NifResult<u64> {
    let factory = c("factory id", &factory_id)?;
    let id = c("id", &id)?;
    let mode = c("if_running", &if_running)?;
    veneer::spawn_request(
        core.enter()?.ctx(),
        token,
        &factory,
        &id,
        payload.as_slice(),
        &mode,
        replay,
        offset,
    )
    .map_err(refused)
}

#[rustler::nif(schedule = "DirtyIo")]
fn spawn_result(
    core: ResourceArc<Core>,
    handle: u64,
    timeout_ms: u64,
) -> NifResult<Option<String>> {
    veneer::spawn_result(core.enter()?.ctx(), handle, timeout_ms).map_err(refused)
}

/// The pending timers of the Cell `token` runs, as `{kind, at}` pairs.
#[rustler::nif]
fn pending_timers(core: ResourceArc<Core>, token: u64) -> NifResult<Vec<(Atom, u64)>> {
    let ops = veneer::pending_timers(core.enter()?.ctx(), token).map_err(refused)?;
    Ok(ops
        .into_iter()
        .map(|op| {
            let kind = match op.kind {
                flz_core::schema::TimerKind::EventTime => atoms::event_time(),
                flz_core::schema::TimerKind::ProcessingTime => atoms::processing_time(),
            };
            (kind, op.scheduled_at)
        })
        .collect())
}

#[rustler::nif]
fn part(core: ResourceArc<Core>, token: u64, item: u32, payload: Binary) -> NifResult<Atom> {
    veneer::part(core.enter()?.ctx(), token, item, payload.as_slice()).map_err(refused)?;
    Ok(atoms::ok())
}

/// `{:kept, result}`, `:run` or `:unrecorded`.
#[rustler::nif]
fn effect_begin<'a>(
    env: Env<'a>,
    core: ResourceArc<Core>,
    token: u64,
    item: u32,
    name: String,
) -> NifResult<Term<'a>> {
    let name = c("effect name", &name)?;
    Ok(
        match veneer::effect_begin(core.enter()?.ctx(), token, item, &name).map_err(refused)? {
            EffectStart::Kept(result) => (atoms::kept(), bin(env, &result)).encode(env),
            EffectStart::Run => atoms::run().encode(env),
            EffectStart::Unrecorded => atoms::unrecorded().encode(env),
        },
    )
}

/// `nil` to return the call's own result, or the result kept first.
#[rustler::nif(schedule = "DirtyIo")]
fn effect_end<'a>(
    env: Env<'a>,
    core: ResourceArc<Core>,
    token: u64,
    item: u32,
    payload: Binary,
) -> NifResult<Term<'a>> {
    match veneer::effect_end(core.enter()?.ctx(), token, item, payload.as_slice())
        .map_err(refused)?
    {
        Some(first) => Ok(bin(env, &first)),
        None => Ok(rustler::types::atom::nil().encode(env)),
    }
}

#[rustler::nif]
fn effect_release(core: ResourceArc<Core>, token: u64, item: u32) -> NifResult<Atom> {
    veneer::effect_release(core.enter()?.ctx(), token, item);
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn admin_call(core: ResourceArc<Core>, verb: String, args: String) -> NifResult<String> {
    let verb = c("verb", &verb)?;
    let args = c("args", &args)?;
    veneer::admin_call(core.enter()?.ctx(), &verb, &args).map_err(refused)
}

#[allow(clippy::too_many_arguments)]
#[rustler::nif(schedule = "DirtyIo")]
fn send_message(
    core: ResourceArc<Core>,
    relay: String,
    channel: String,
    message: Binary,
    msg_id: String,
    key: String,
    local: bool,
) -> NifResult<String> {
    let relay = c("relay", &relay)?;
    let channel = c("entry channel", &channel)?;
    let msg_id = c("message id", &msg_id)?;
    let key = c("key", &key)?;
    veneer::send_message(
        core.enter()?.ctx(),
        &relay,
        &channel,
        message.as_slice(),
        &msg_id,
        &key,
        local,
    )
    .map_err(refused)
}

// --- the local run -----------------------------------------------------------------

#[rustler::nif]
fn local_start(core: ResourceArc<Core>, factory_id: String, relay_id: String) -> NifResult<Atom> {
    let factory = c("factory id", &factory_id)?;
    let relay = c("relay id", &relay_id)?;
    core.begin(|ctx| veneer::local_start(ctx, &factory, &relay))
}

#[rustler::nif]
fn local_on_sink(core: ResourceArc<Core>, channel: String) -> NifResult<Atom> {
    let channel = c("output channel", &channel)?;
    veneer::local_on_sink(core.enter()?.ctx(), &channel).map_err(refused)?;
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_feed(core: ResourceArc<Core>, channel: String, payload: Binary) -> NifResult<Atom> {
    let channel = c("entry channel", &channel)?;
    veneer::local_feed(core.enter()?.ctx(), &channel, payload.as_slice()).map_err(refused)?;
    Ok(atoms::ok())
}

/// The next released sink output as an output record, or `nil`.
#[rustler::nif(schedule = "DirtyIo")]
fn local_next_output<'a>(
    env: Env<'a>,
    core: ResourceArc<Core>,
    timeout_ms: u64,
) -> NifResult<Term<'a>> {
    match veneer::local_next_output(core.enter()?.ctx(), timeout_ms).map_err(refused)? {
        Some(encoded) => {
            let output = flz_core::schema::decode_output(&encoded)
                .map_err(|e| failed(Fail(format!("output failed to decode: {e}"))))?;
            shape::sink_output(&Sink(env), output).map_err(failed)
        }
        None => Ok(rustler::types::atom::nil().encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_run_until_idle(core: ResourceArc<Core>, timeout_ms: u64) -> NifResult<Atom> {
    veneer::local_run_until_idle(core.enter()?.ctx(), timeout_ms).map_err(refused)?;
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn local_stop(core: ResourceArc<Core>) -> NifResult<Atom> {
    veneer::local_stop(core.enter()?.ctx());
    Ok(atoms::ok())
}

rustler::init!("Elixir.Flezha.SDK.Native");
