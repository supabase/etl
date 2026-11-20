"""Control only this disposable container's DNS answers and Postgres reachability."""

import asyncio
import json
import os
import pathlib
import re
import signal
import subprocess

ADDRESS_A = "fd42:6574:6c::11"
ADDRESS_B = "fd42:6574:6c::12"
ADDRESS_V4 = "172.30.248.10"
MODES = {"a", "b", "v4", "mixed", "reject4", "drop4", "reject6", "drop6", "drop_all", "drop_dns"}


def command(*args):
    """Run a fixture operation, failing the control request on errors."""
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def hosts_for(mode):
    """Return the complete authoritative test-host mapping for one scenario."""
    if mode not in MODES:
        raise ValueError("Unknown network scenario")
    address = ADDRESS_B if mode == "b" else ADDRESS_A
    hosts = "fd42:6574:6c::10 admin.test\n"
    if mode != "v4":
        hosts += f"{address} source.test wrong.test\n"
    if mode in MODES - {"a", "b", "drop_dns"}:
        hosts += f"{ADDRESS_V4} source.test wrong.test\n"
    return hosts


class Controller:
    """Serialize network transitions and acknowledge them after application."""

    def __init__(self, dns):
        self.dns = dns
        self.mode = "a"
        self.retired_backends = 0
        self.lock = asyncio.Lock()

    def change(self, mode):
        """Keep Postgres alive while replacing DNS and removing old paths."""
        hosts = hosts_for(mode)
        command("iptables", "-F", "INPUT")
        command("ip6tables", "-F", "INPUT")
        if mode == "drop_dns":
            for protocol in ("udp", "tcp"):
                command("iptables", "-A", "INPUT", "-p", protocol, "--dport", "53", "-j", "DROP")
        if mode in {"reject4", "drop4", "drop_all"}:
            action = ["REJECT", "--reject-with", "tcp-reset"] if mode == "reject4" else ["DROP"]
            command("iptables", "-A", "INPUT", "-d", ADDRESS_V4, "-p", "tcp",
                    "--dport", "5432", "-j", *action)
        if mode in {"reject6", "drop6", "drop_all"}:
            action = ["REJECT", "--reject-with", "tcp-reset"] if mode == "reject6" else ["DROP"]
            command("ip6tables", "-A", "INPUT", "-d", ADDRESS_A, "-p", "tcp",
                    "--dport", "5432", "-j", *action)
        if mode == "b":
            command("ip6tables", "-A", "INPUT", "-d", ADDRESS_A, "-p", "tcp",
                    "--dport", "5432", "-j", "REJECT", "--reject-with", "tcp-reset")
        pathlib.Path("/run/dns-hosts").write_text(hosts)
        self.dns.send_signal(signal.SIGHUP)
        if mode == "b":
            # SIGTERM is PostgreSQL's normal backend-termination signal. Unlike
            # ss -K, this also works on kernels without TCP socket destruction.
            # Select only server sockets bound to A; admin sessions stay alive.
            # Inspect as the backend owner; container root lacks SYS_PTRACE.
            sockets = command("runuser", "-u", "postgres", "--", "ss", "-6", "-Hntp",
                              "src", f"[{ADDRESS_A}]", "sport", "=", ":5432")
            pids = {int(pid) for pid in re.findall(r'"postgres",pid=(\d+)', sockets)}
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            self.retired_backends = len(pids)
        self.mode = mode

    async def handle(self, reader, writer):
        """Accept one bounded JSON request and return structural diagnostics."""
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5)
            request = json.loads(raw)
            async with self.lock:
                if "mode" in request:
                    self.change(request["mode"])
                    # dnsmasq handles SIGHUP asynchronously; tests also poll resolution.
                    await asyncio.sleep(0.1)
                response = {"mode": self.mode,
                            "retired_backends": self.retired_backends,
                            "ipv4_rules": command("iptables", "-nvL", "INPUT"),
                            "ipv6_rules": command("ip6tables", "-nvL", "INPUT")}
        except Exception as error:
            response = {"error": type(error).__name__}
        writer.write(json.dumps(response).encode() + b"\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()


async def main():
    """Start isolated DNS and the lab control listener."""
    assert ADDRESS_A in hosts_for("a") and ADDRESS_B not in hosts_for("a")
    assert ADDRESS_B in hosts_for("b") and ADDRESS_A not in hosts_for("b")
    assert ADDRESS_V4 in hosts_for("mixed")
    pathlib.Path("/run/dns-hosts").write_text(hosts_for("a"))
    dns = subprocess.Popen([
        "dnsmasq", "--keep-in-foreground", "--no-hosts", "--local=/test/",
        "--addn-hosts=/run/dns-hosts", "--local-ttl=0", "--cache-size=0",
        "--log-queries", "--log-facility=-", "--user=root",
    ])
    try:
        controller = Controller(dns)
        server = await asyncio.start_server(controller.handle, "0.0.0.0", 8000, limit=4096)
        pathlib.Path("/run/lab-ready").touch()
        async with server:
            await server.serve_forever()
    finally:
        dns.terminate()
        dns.wait(timeout=5)


if __name__ == "__main__":
    asyncio.run(main())
