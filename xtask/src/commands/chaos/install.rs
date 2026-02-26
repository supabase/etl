use anyhow::Result;

use super::assert_safe_context;

const HELM_REPO_NAME: &str = "chaos-mesh";
const HELM_REPO_URL: &str = "https://charts.chaos-mesh.org";
const HELM_RELEASE: &str = "chaos-mesh";
const HELM_NAMESPACE: &str = "chaos-mesh";

/// Socket path for each supported container runtime.
fn socket_path(runtime: &str) -> Result<&'static str> {
    match runtime {
        "docker" => Ok("/var/run/docker.sock"),
        "containerd" => Ok("/run/containerd/containerd.sock"),
        "crio" => Ok("/var/run/crio/crio.sock"),
        other => {
            anyhow::bail!("unknown runtime '{other}', supported values: docker, containerd, crio")
        }
    }
}

pub(crate) fn install(runtime: &str) -> Result<()> {
    assert_safe_context();

    let socket = socket_path(runtime)?;

    println!("Adding Chaos Mesh helm repo…");
    // Ignore errors — repo may already be registered.
    let _ = run(&["helm", "repo", "add", HELM_REPO_NAME, HELM_REPO_URL]);

    println!("Updating helm repo…");
    run(&["helm", "repo", "update", HELM_REPO_NAME])?;

    println!("Installing Chaos Mesh (runtime={runtime}, socket={socket})…");
    run(&[
        "helm",
        "upgrade",
        "--install",
        HELM_RELEASE,
        &format!("{HELM_REPO_NAME}/{HELM_RELEASE}"),
        "--namespace",
        HELM_NAMESPACE,
        "--create-namespace",
        "--set",
        &format!("chaosDaemon.runtime={runtime}"),
        "--set",
        &format!("chaosDaemon.socketPath={socket}"),
    ])?;

    println!("Waiting for Chaos Mesh pods to become ready…");
    run(&[
        "kubectl",
        "rollout",
        "status",
        "deployment/chaos-controller-manager",
        "-n",
        HELM_NAMESPACE,
        "--timeout=120s",
    ])?;
    run(&[
        "kubectl",
        "rollout",
        "status",
        "daemonset/chaos-daemon",
        "-n",
        HELM_NAMESPACE,
        "--timeout=120s",
    ])?;

    println!("Chaos Mesh installed successfully.");
    Ok(())
}

fn run(argv: &[&str]) -> Result<()> {
    let (program, args) = argv.split_first().expect("argv must not be empty");
    let status = std::process::Command::new(program)
        .args(args)
        .status()
        .map_err(|e| anyhow::anyhow!("failed to run '{}': {e}", program))?;
    if !status.success() {
        anyhow::bail!("'{}' exited with {status}", argv.join(" "));
    }
    Ok(())
}
