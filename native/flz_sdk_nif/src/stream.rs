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

/// Queue a frame only while the writer is fewer than `max_pending` frames
/// behind, and refuse it with `busy` otherwise: a part, never waited on and
/// never held, is dropped rather than grow the queue of a stream that stalled.
#[rustler::nif]
fn try_send_frame<'a>(
    env: Env<'a>,
    stream: ClientStreamArc,
    payload_id: String,
    message: rustler::Binary<'a>,
    metadata: HashMap<String, String>,
    max_pending: usize,
) -> NifResult<Term<'a>> {
    let command = Command::Send {
        payload_id,
        message: message.as_slice().to_vec(),
        metadata,
    };

    Ok(match try_queue(&stream.commands, max_pending, command) {
        Ok(()) => atoms::ok().encode(env),
        Err(()) => (atoms::error(), atoms::busy()).encode(env),
    })
}

/// The bounded half of the writer's queue, for the frames that may be dropped.
pub(crate) fn try_queue(
    commands: &Sender<Command>,
    max_pending: usize,
    command: Command,
) -> Result<(), ()> {
    if commands.len() >= max_pending {
        return Err(());
    }
    commands.send(command).map_err(|_| ())
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

#[cfg(test)]
mod tests {
    use crossbeam::channel::unbounded;

    use super::*;

    fn part() -> Command {
        Command::Send {
            payload_id: "output_part".to_string(),
            message: b"x".to_vec(),
            metadata: HashMap::new(),
        }
    }

    /// A writer that stopped draining: the queue fills to the bound and every
    /// part past it is refused at once rather than queued.
    #[test]
    fn a_part_behind_a_stalled_writer_is_refused_at_the_bound() {
        let (commands, stalled) = unbounded::<Command>();
        for _ in 0..3 {
            assert!(try_queue(&commands, 3, part()).is_ok());
        }
        assert!(try_queue(&commands, 3, part()).is_err());
        assert_eq!(stalled.len(), 3, "a refused part is not queued");
    }

    #[test]
    fn a_writer_gone_refuses_the_part() {
        let (commands, gone) = unbounded::<Command>();
        drop(gone);
        assert!(try_queue(&commands, 3, part()).is_err());
    }
}
