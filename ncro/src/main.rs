use pound::Parse as _;

mod cli;
mod warn_buffer;

fn main() -> misstep::Result<()> {
  caress::handle(cli::Args::SPEC);
  misstep::install_panic_hook();
  cli::run()
}
