//! IPv4 and IPv6 listeners for health and telemetry endpoints.

use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

use libc::{EAFNOSUPPORT, EPROTONOSUPPORT};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;

/// Maximum number of pending connections, subject to the host's limit.
const LISTEN_BACKLOG: i32 = 1024;

/// Binds a dual-stack TCP listener, falling back to IPv4 if IPv6 is
/// unsupported.
///
/// Enables IPv4 on the IPv6 socket explicitly rather than relying on host
/// defaults. Permission, resource, and bind errors propagate unchanged.
/// Must be called from a Tokio runtime with I/O enabled.
pub fn bind_listener(port: u16) -> io::Result<TcpListener> {
    let (socket, address) = match Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP)) {
        Ok(socket) => {
            socket.set_only_v6(false)?;

            (socket, SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)))
        }
        Err(error) if matches!(error.raw_os_error(), Some(EAFNOSUPPORT | EPROTONOSUPPORT)) => (
            Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?,
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)),
        ),
        Err(error) => return Err(error),
    };

    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    socket.listen(LISTEN_BACKLOG)?;

    TcpListener::from_std(socket.into())
}
