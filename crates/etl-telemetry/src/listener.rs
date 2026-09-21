//! Listen addresses for health and telemetry endpoints.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

#[cfg(unix)]
use libc::{EAFNOSUPPORT, EPROTONOSUPPORT};
use tokio::net::TcpSocket;
#[cfg(windows)]
use windows_sys::Win32::Networking::WinSock::{
    WSAEAFNOSUPPORT as EAFNOSUPPORT, WSAEPROTONOSUPPORT as EPROTONOSUPPORT,
};

/// Selects `[::]:port`, or `0.0.0.0:port` when IPv6 sockets are unsupported.
///
/// Checks socket support without binding a port. Permission and resource errors
/// propagate, and callers remain responsible for reporting bind failures.
/// IPv4 connections to an IPv6 listener follow the host's dual-stack settings.
pub fn listen_address(port: u16) -> io::Result<SocketAddr> {
    let ip = match TcpSocket::new_v6() {
        Ok(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        Err(error) if matches!(error.raw_os_error(), Some(EAFNOSUPPORT | EPROTONOSUPPORT)) => {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        }
        Err(error) => return Err(error),
    };

    Ok(SocketAddr::new(ip, port))
}
