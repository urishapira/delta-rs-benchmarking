use async_trait::async_trait;
use bench_common::{run_benchmark, DeltaLoader};
use deltalake::DeltaTableBuilder;
use url::Url;

struct Loader;

#[async_trait]
impl DeltaLoader for Loader {
    async fn discover_version(&self, table_url: Url) -> Result<i64, String> {
        // NOTE: in deltalake 0.16.x, from_uri returns a builder (not Result), so no map_err here.
        let t = DeltaTableBuilder::from_uri(table_url.as_str())
            .without_files()
            .load()
            .await
            .map_err(|e| format!("Failed discovering latest version: {e}"))?;

        // In older versions this is typically i64.
        Ok(t.version())
    }

    async fn load(&self, table_url: Url, version: i64, require_files: bool) -> Result<(), String> {
        let mut b = DeltaTableBuilder::from_uri(table_url.as_str()).with_version(version);

        if !require_files {
            b = b.without_files();
        }

        let _t = b.load().await.map_err(|e| format!("{e}"))?;
            println!("require_files (requested={}) => actual={}", require_files, _t.config.require_files);

        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), String> {
    run_benchmark("old", &Loader).await
}