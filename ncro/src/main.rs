mod cli;
mod warn_buffer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  cli::run().await
}
