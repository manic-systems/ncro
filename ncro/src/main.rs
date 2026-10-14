mod cli;
mod warn_buffer;

#[tokio::main]
async fn main() -> misstep::Result<()> {
  misstep::install_panic_hook();
  cli::run().await
}
