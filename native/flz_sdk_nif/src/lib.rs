//! The Elixir SDK's socket client: UDS and QUIC client streams over
//! `flz_transport`, delivered to Elixir processes as
//! `{:flz_data, stream_id, payload_id, message, metadata}` and
//! `{:flz_closed, stream_id, reason}`. Framing, QUIC and TLS stay in
//! `flz_transport`, where every runtime shares them.

mod atoms;
mod client;
mod message;
mod stream;

#[allow(non_local_definitions)]
fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    let _ = rustler::resource!(stream::ClientStream, env);
    let _ = rustler::resource!(client::QuicClientConn, env);
    true
}

rustler::init!("Elixir.Flezha.SDK.Native", load = load);
