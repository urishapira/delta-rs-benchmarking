use bench_common::{run_benchmark, DeltaLoader};
use deltalake::DeltaTableBuilder;

struct Loader;

#[async_trait::async_trait]
impl DeltaLoader for Loader {
    async fn discover_version(&self, table_url: url::Url) -> Result<i64, String> {
        let t = DeltaTableBuilder::from_url(table_url)
            .map_err(|e| format!("Failed building table: {e}"))?
            .without_files()
            .load()
            .await
            .map_err(|e| format!("Failed discovering latest version: {e}"))?;
        t.version().ok_or_else(|| "Loaded table but version is None".to_string())
    }

    async fn load(
        &self,
        table_url: url::Url,
        version: i64,
        require_files: bool,
    ) -> Result<(), String> {
        let mut b = DeltaTableBuilder::from_url(table_url)
            .map_err(|e| format!("Failed building table: {e}"))?
            .with_version(version);

        if !require_files {
            b = b.without_files();
        }

        let _t = b.load().await.map_err(|e| format!("{e}"))?;
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), String> {
    run_benchmark("new", &Loader).await
}