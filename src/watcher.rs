use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result};
use tokio::fs;
use tracing::warn;

use crate::pod::{Pod, PodName};

pub async fn scan(manifest_dir: &Path) -> Result<HashMap<PodName, Pod>> {
    let mut pods = HashMap::new();
    let mut entries = fs::read_dir(manifest_dir)
        .await
        .with_context(|| format!("reading manifests dir {manifest_dir:?}"))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .context("iterating manifests dir entries")?
    {
        let path = entry.path();
        if !is_yaml_file(&path) {
            continue;
        }

        let yaml = match fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = ?e, ?path, "skipping unreadable manifest");
                continue;
            }
        };

        let pod = match Pod::from_yaml(&yaml) {
            Ok(pod) => pod,
            Err(e) => {
                warn!(error = ?e, ?path, "skipping malformed manifest");
                continue;
            }
        };

        let name = pod.metadata.name.clone();
        if pods.insert(name.clone(), pod).is_some() {
            warn!(name = %name, ?path, "duplicate Pod name across manifest files; later file wins");
        }
    }

    Ok(pods)
}

fn is_yaml_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("yaml") | Some("yml")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Unique per-test tempdir so parallel `cargo test` runs don't collide.
    fn unique_temp_dir(label: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("my-k8s-test-watcher-{label}-{pid}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_file(dir: &Path, name: &str, contents: &str) {
        std::fs::write(dir.join(name), contents).unwrap();
    }

    /// Minimal Pod YAML with the given name. Spec just runs sleep.
    fn pod_yaml(name: &str) -> String {
        format!(
            r#"apiVersion: v1
kind: Pod
metadata:
  name: {name}
spec:
  containers:
    - name: c1
      image: busybox
      command: ["sleep", "infinity"]
"#
        )
    }

    #[tokio::test]
    async fn empty_dir_returns_empty_map() {
        let dir = unique_temp_dir("empty");
        let pods = scan(&dir).await.expect("scan should succeed");
        assert!(pods.is_empty());
    }

    #[tokio::test]
    async fn parses_single_pod_yaml() {
        let dir = unique_temp_dir("single");
        write_file(&dir, "web.yaml", &pod_yaml("web"));
        let pods = scan(&dir).await.expect("scan should succeed");
        assert_eq!(pods.len(), 1);
        assert!(pods.contains_key("web"));
    }

    #[tokio::test]
    async fn parses_multiple_pod_yamls() {
        let dir = unique_temp_dir("multi");
        write_file(&dir, "web.yaml", &pod_yaml("web"));
        write_file(&dir, "worker.yaml", &pod_yaml("worker"));
        let pods = scan(&dir).await.expect("scan should succeed");
        assert_eq!(pods.len(), 2);
        assert!(pods.contains_key("web"));
        assert!(pods.contains_key("worker"));
    }

    #[tokio::test]
    async fn accepts_both_yaml_and_yml_extensions() {
        let dir = unique_temp_dir("ext");
        write_file(&dir, "web.yaml", &pod_yaml("web"));
        write_file(&dir, "worker.yml", &pod_yaml("worker"));
        let pods = scan(&dir).await.expect("scan should succeed");
        assert_eq!(pods.len(), 2);
    }

    #[tokio::test]
    async fn skips_non_yaml_files() {
        let dir = unique_temp_dir("nonyaml");
        write_file(&dir, "web.yaml", &pod_yaml("web"));
        write_file(&dir, "README.md", "ignore me");
        write_file(&dir, "notes.txt", "ignore me too");
        let pods = scan(&dir).await.expect("scan should succeed");
        assert_eq!(pods.len(), 1);
        assert!(pods.contains_key("web"));
    }

    #[tokio::test]
    async fn skips_malformed_yaml_but_continues() {
        let dir = unique_temp_dir("malformed");
        write_file(&dir, "broken.yaml", "this is not a valid Pod");
        write_file(&dir, "good.yaml", &pod_yaml("good"));
        let pods = scan(&dir)
            .await
            .expect("scan should succeed even when one file is bad");
        assert_eq!(pods.len(), 1);
        assert!(pods.contains_key("good"));
    }

    #[tokio::test]
    async fn last_file_wins_on_duplicate_pod_name() {
        let dir = unique_temp_dir("dup");
        write_file(&dir, "a.yaml", &pod_yaml("dup"));
        write_file(&dir, "b.yaml", &pod_yaml("dup"));
        let pods = scan(&dir).await.expect("scan should succeed");
        assert_eq!(pods.len(), 1, "both files had the same Pod name");
    }

    #[tokio::test]
    async fn errors_on_missing_directory() {
        let result = scan(Path::new("/nonexistent-dir-xyz-12345")).await;
        assert!(result.is_err(), "missing dir should produce an error");
    }
}
