//! Connecting: where the controller is, dialling it over UDS or QUIC, and the
//! two threads each connected stream runs.
//!
//! Each stream runs a blocking reader thread, decoding frames with
//! `flz_transport`'s payloader and delivering them to the stream's current
//! owner, and a writer thread draining its command channel.

use std::io::Write;
use std::net::Shutdown;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(unix)]
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver};
use flz_transport::service::payloader::{build_from_binary_into, extract_payload};
use flz_transport::transport::close_reason::CloseReason;
use flz_transport::transport::config::{self as transport_config, QuicEntry, TransportType};
use flz_transport::transport::protocol::{STREAM_TYPE_PRIMARY, STREAM_TYPE_SECONDARY};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::socket_dir::{self, Role};
use flz_transport::utility::socket_probe;
use rustler::{Encoder, Env, LocalPid, NifResult, ResourceArc, Term};

use crate::atoms;
use crate::message::{send_closed_msg, send_data_msg};
use crate::stream::{ClientStream, ClientStreamArc, Command};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// A QUIC client connection, held by Elixir so it can open data streams on it.
pub struct QuicClientConn {
    pub client: QuicClient,
}

pub type QuicClientConnArc = ResourceArc<QuicClientConn>;

/// Connect a UDS client stream with the same bounded retry the Rust client uses
/// (exponential from 10 ms, cap 500 ms, ~3 s deadline — the controller binds a
/// data socket immediately before `ConnectSocket`, and bind+connect races).
#[rustler::nif(schedule = "DirtyIo")]
fn client_connect_uds<'a>(
    env: Env<'a>,
    socket_path: String,
    owner_pid: LocalPid,
) -> NifResult<Term<'a>> {
    #[cfg(unix)]
    let connected = connect_uds_with_retry(&socket_path).map(|stream| {
        // The directory that answered is where this runner's later sockets
        // are opened, and where a reconnect returns to — the same slot the
        // Rust client records.
        socket_dir::note_connected(&socket_path);
        spawn_client_stream(FlezhaStream::unix(stream), owner_pid)
    });

    #[cfg(not(unix))]
    let connected: std::io::Result<ClientStreamArc> = {
        let _ = owner_pid;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("{socket_path}: this platform has no Unix domain sockets; connect with QUIC"),
        ))
    };

    Ok(match connected {
        Ok(handle) => (atoms::ok(), handle).encode(env),
        Err(e) => (atoms::error(), e.to_string()).encode(env),
    })
}

/// Where this runner's controller is: the resolved absolute socket directory
/// and the QUIC candidate list, both from `flz_transport`, the same ladder every
/// runner walks.
///
/// `socket_dir` and `quic_addresses` carry the CALLER's reading of the two
/// variables: the BEAM keeps its own environment table, so a value set with
/// `System.put_env/2` is invisible to `std::env::var` here. Passing them keeps
/// the rule in one place while letting the caller's environment win.
#[rustler::nif(schedule = "DirtyIo")]
fn resolve_connect<'a>(
    env: Env<'a>,
    socket_dir: Option<String>,
    quic_addresses: Option<String>,
) -> NifResult<Term<'a>> {
    let resolved_dir = match socket_dir::resolve_socket_dir(Role::Runner, socket_dir.as_deref()) {
        Ok(resolved) => resolved.path.display().to_string(),
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };

    let entries = match transport_config::quic_addresses_with(quic_addresses.as_deref()) {
        Ok(entries) => entries.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };

    Ok((atoms::ok(), resolved_dir, entries).encode(env))
}

/// A data-socket path in `socket_dir`, composed by `flz_transport`: the
/// runner-id field is bounded there and a path a Unix domain socket address
/// cannot hold is refused by name and length.
#[rustler::nif]
fn stream_socket_path(
    socket_dir: String,
    runner_id: String,
    stream_type: String,
) -> Result<String, String> {
    socket_dir::stream_socket_path_in(Path::new(&socket_dir), &runner_id, &stream_type)
        .map_err(|refusal| refusal.to_string())
}

/// The control-socket path in `socket_dir`, composed by `flz_transport`.
#[rustler::nif]
fn control_socket_path_in(socket_dir: String) -> Result<String, String> {
    socket_dir::control_socket_path_in(Path::new(&socket_dir))
        .map_err(|refusal| refusal.to_string())
}

/// Why this node cannot reach a controller over `transport`, in the words the
/// Rust runner and `flz runner check` use. `socket_dir` and `quic_addresses`
/// carry the caller's environment view, as in `resolve_connect/2`.
#[rustler::nif(schedule = "DirtyIo")]
fn connect_diagnosis(
    transport: String,
    socket_dir: Option<String>,
    quic_addresses: Option<String>,
) -> String {
    let transport = match transport.as_str() {
        "quic" => TransportType::Quic,
        _ => TransportType::Uds,
    };

    socket_probe::connect_diagnosis(transport, socket_dir.as_deref(), quic_addresses.as_deref())
}

/// Open a QUIC client connection, verifying the server against `ca_cert_path`.
/// The entry is resolved here, on this attempt — never parsed once and cached.
#[rustler::nif(schedule = "DirtyIo")]
fn client_connect_quic<'a>(
    env: Env<'a>,
    address: String,
    ca_cert_path: String,
) -> NifResult<Term<'a>> {
    let entry = match QuicEntry::parse(&address) {
        Ok(entry) => entry,
        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
    };
    let (addrs, mut failures) = transport_config::resolve_entries(&[entry]);
    if addrs.is_empty() {
        return Ok((atoms::error(), failures.join("; ")).encode(env));
    }

    for addr in addrs {
        match QuicClient::connect(addr, &ca_cert_path) {
            Ok(client) => {
                let conn = ResourceArc::new(QuicClientConn { client });
                return Ok((atoms::ok(), conn).encode(env));
            }
            Err(e) => failures.push(format!("{}: {}", addr, e)),
        }
    }

    Ok((atoms::error(), failures.join("; ")).encode(env))
}

/// Open a bidirectional QUIC stream and write the 2-byte `[type, index]` preamble
/// (types 1/2); type 0 is the control stream and writes no preamble.
#[rustler::nif(schedule = "DirtyIo")]
fn client_open_stream<'a>(
    env: Env<'a>,
    conn: QuicClientConnArc,
    stream_type: u8,
    index: u8,
    owner_pid: LocalPid,
) -> NifResult<Term<'a>> {
    match conn.client.open_bidi_stream() {
        Ok(mut bidi) => {
            if stream_type == STREAM_TYPE_PRIMARY || stream_type == STREAM_TYPE_SECONDARY {
                if let Err(e) = bidi.write_all(&[stream_type, index]) {
                    return Ok((atoms::error(), e.to_string()).encode(env));
                }
                let _ = bidi.flush();
            }

            let handle = spawn_client_stream(FlezhaStream::Quic(bidi), owner_pid);
            Ok((atoms::ok(), handle).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

#[cfg(unix)]
fn connect_uds_with_retry(path: &str) -> std::io::Result<UnixStream> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut attempt: u32 = 0;

    loop {
        match UnixStream::connect(path) {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e);
                }

                let backoff = std::cmp::min(10u64 * (1u64 << attempt.min(6)), 500);
                thread::sleep(Duration::from_millis(backoff));
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

fn spawn_client_stream(stream: FlezhaStream, owner_pid: LocalPid) -> ClientStreamArc {
    let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let owner = Arc::new(Mutex::new(owner_pid));
    let (commands, received) = unbounded::<Command>();

    let read_stream = stream
        .clone_handle()
        .expect("clone client stream for reader");
    spawn_reader(read_stream, stream_id, Arc::clone(&owner));
    spawn_writer(stream, received);

    ResourceArc::new(ClientStream {
        stream_id,
        commands,
        owner,
    })
}

/// Reader: decode frames and deliver each to the stream's owner at that moment,
/// so a `controlling_process/2` takes effect from the next frame.
fn spawn_reader(mut read_stream: FlezhaStream, stream_id: u64, owner: Arc<Mutex<LocalPid>>) {
    thread::spawn(move || loop {
        match extract_payload(&mut read_stream) {
            Ok((payload_id, message, metadata)) => {
                let who = *owner.lock().unwrap();
                send_data_msg(who, stream_id, &payload_id, &message, &metadata);
            }

            Err(e) => {
                let who = *owner.lock().unwrap();
                send_closed_msg(who, stream_id, &CloseReason::from(e.kind()));
                break;
            }
        }
    });
}

/// Writer: build and write each queued frame. A frame the encoder refuses (a
/// metadata field longer than the wire carries) is dropped and the connection
/// kept, because the same value would be refused identically for ever.
fn spawn_writer(mut write_stream: FlezhaStream, commands: Receiver<Command>) {
    thread::spawn(move || {
        let mut buf = Vec::new();

        while let Ok(command) = commands.recv() {
            match command {
                Command::Send {
                    payload_id,
                    message,
                    metadata,
                } => {
                    buf.clear();
                    if let Err(e) =
                        build_from_binary_into(&mut buf, &payload_id, &message, &metadata)
                    {
                        eprintln!("[flz_sdk_nif] frame refused for payload {payload_id}: {e}");
                        continue;
                    }
                    if write_stream.write_all(&buf).is_err() {
                        break;
                    }
                }

                Command::Close => {
                    let _ = write_stream.shutdown(Shutdown::Both);
                    break;
                }
            }
        }
    });
}

#[cfg(all(test, unix))]
mod tests {
    use std::collections::HashMap;
    use std::os::unix::net::UnixListener;

    use super::*;

    #[test]
    fn connect_retry_succeeds_against_a_live_listener() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("flz-client-test-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _listener = UnixListener::bind(&path).unwrap();

        let stream = connect_uds_with_retry(path.to_str().unwrap());
        assert!(stream.is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn connect_retry_gives_up_after_the_deadline_on_a_missing_socket() {
        let path = "/nonexistent/dir/definitely/not/a/socket.sock";
        let start = Instant::now();
        let result = connect_uds_with_retry(path);
        assert!(result.is_err());
        // The ~3 s deadline bounds the wait (allow slack for slow CI).
        assert!(start.elapsed() <= Duration::from_secs(5));
    }

    // One encoder serves both directions: the frame a runner's writer builds is
    // refused by the same check that refuses the controller's. The frame is
    // dropped; the connection carries on.
    #[test]
    fn writer_drops_a_refused_frame_and_keeps_the_connection() {
        let (near, far) = UnixStream::pair().unwrap();
        let (tx, rx) = unbounded();
        spawn_writer(FlezhaStream::unix(near), rx);

        let mut oversized = HashMap::new();
        oversized.insert("flz.key".to_string(), "v".repeat(256));
        tx.send(Command::Send {
            payload_id: "refused".to_string(),
            message: b"body".to_vec(),
            metadata: oversized,
        })
        .unwrap();
        tx.send(Command::Send {
            payload_id: "accepted".to_string(),
            message: b"body".to_vec(),
            metadata: HashMap::new(),
        })
        .unwrap();

        // Only the second frame is on the wire, so the first byte-run read
        // back is the one the encoder accepted.
        let (payload_id, message, _) = extract_payload(&far).unwrap();
        assert_eq!(
            (payload_id.as_str(), message.as_slice()),
            ("accepted", &b"body"[..])
        );
    }

    #[test]
    fn stream_type_preamble_constants_match_the_protocol() {
        assert_eq!(STREAM_TYPE_PRIMARY, 1);
        assert_eq!(STREAM_TYPE_SECONDARY, 2);
    }
}
