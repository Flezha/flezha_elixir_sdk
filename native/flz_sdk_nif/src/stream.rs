//! A client stream as Elixir holds it, and the per-stream operations on it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crossbeam::channel::Sender;
use rustler::{Encoder, Env, LocalPid, NifResult, ResourceArc, Term};

use crate::atoms;

/// What the stream's writer thread is asked to do.
pub(crate) enum Command {
    Send {
        payload_id: String,
        message: Vec<u8>,
        metadata: HashMap<String, String>,
    },
    Close,
}

/// A connected client stream. Its reader thread delivers to whoever `owner`
/// names at the moment a frame arrives; its writer thread drains `commands`.
pub struct ClientStream {
    pub(crate) stream_id: u64,
    pub(crate) commands: Sender<Command>,
    pub(crate) owner: Arc<Mutex<LocalPid>>,
}

impl Drop for ClientStream {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Close);
    }
}

pub(crate) type ClientStreamArc = ResourceArc<ClientStream>;

/// Queue a frame; the writer builds and writes it. The only way a queue fails
/// is that the writer is gone, and then the frame is lost, which is the answer.
#[rustler::nif]
fn send_frame<'a>(
    env: Env<'a>,
    stream: ClientStreamArc,
    payload_id: String,
    message: rustler::Binary<'a>,
    metadata: HashMap<String, String>,
) -> NifResult<Term<'a>> {
    let queued = stream.commands.send(Command::Send {
        payload_id,
        message: message.as_slice().to_vec(),
        metadata,
    });

    Ok(match queued {
        Ok(()) => atoms::ok().encode(env),
        Err(_) => (atoms::error(), atoms::driver_gone()).encode(env),
    })
}

/// A client stream is active from birth: its reader delivers every frame to the
/// current owner, so there is nothing to switch on.
#[rustler::nif]
fn set_active(env: Env, _stream: ClientStreamArc) -> NifResult<Term> {
    Ok(atoms::ok().encode(env))
}

#[rustler::nif]
fn close(env: Env, stream: ClientStreamArc) -> NifResult<Term> {
    let _ = stream.commands.send(Command::Close);
    Ok(atoms::ok().encode(env))
}

/// The id carried by this stream's `{:flz_data, ...}` and `{:flz_closed, ...}`.
#[rustler::nif]
fn stream_id(stream: ClientStreamArc) -> u64 {
    stream.stream_id
}

/// Deliver this stream's later frames to `new_owner`.
#[rustler::nif]
fn controlling_process(env: Env, stream: ClientStreamArc, new_owner: LocalPid) -> NifResult<Term> {
    *stream.owner.lock().unwrap() = new_owner;
    Ok(atoms::ok().encode(env))
}
