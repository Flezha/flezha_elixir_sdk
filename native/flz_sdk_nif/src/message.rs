//! The two messages a client stream sends its owner.

use std::collections::HashMap;

use flz_transport::transport::close_reason::CloseReason;
use rustler::{Encoder, Env, LocalPid, OwnedEnv, Term};

use crate::atoms;

fn send_tuple(owner: LocalPid, elements: impl for<'a> FnOnce(Env<'a>) -> Vec<Term<'a>>) {
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&owner, |env| {
        rustler::types::tuple::make_tuple(env, &elements(env))
    });
}

/// `{:flz_data, stream_id, payload_id, message, metadata}`.
pub(crate) fn send_data_msg(
    owner: LocalPid,
    stream_id: u64,
    payload_id: &str,
    message: &[u8],
    metadata: &HashMap<String, String>,
) {
    send_tuple(owner, |env| {
        let pairs: Vec<(Term, Term)> = metadata
            .iter()
            .map(|(k, v)| (k.as_str().encode(env), v.as_str().encode(env)))
            .collect();

        let metadata_term = Term::map_from_pairs(env, &pairs)
            .unwrap_or_else(|_| rustler::types::atom::nil().encode(env));

        let mut binary = rustler::NewBinary::new(env, message.len());
        binary.as_mut_slice().copy_from_slice(message);

        vec![
            atoms::flz_data().encode(env),
            stream_id.encode(env),
            payload_id.encode(env),
            rustler::Binary::from(binary).encode(env),
            metadata_term,
        ]
    });
}

/// `{:flz_closed, stream_id, reason}`, the reason rendered from the vocabulary
/// the controller's drivers report through.
pub(crate) fn send_closed_msg(owner: LocalPid, stream_id: u64, reason: &CloseReason) {
    let reason = reason.to_string();
    send_tuple(owner, |env| {
        vec![
            atoms::flz_closed().encode(env),
            stream_id.encode(env),
            reason.as_str().encode(env),
        ]
    });
}
