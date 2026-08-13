use clap::Parser;
use color_eyre::Report;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Verbosity log
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

const VERBOSE_LEVELS: &[&str] = &["info", "debug", "trace"];

macro_rules! pkg_name {
    () => {
        env!("CARGO_PKG_NAME").replace('-', "_")
    };
}
pub fn initialize() -> Result<Args, Report> {
    use tracing_error::ErrorLayer;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{fmt, EnvFilter};

    color_eyre::install()?;

    let args = Args::parse();

    let crate_level = args
        .verbose
        .min(VERBOSE_LEVELS.len() as u8)
        .checked_sub(1)
        .map(|i| VERBOSE_LEVELS[i as usize])
        .unwrap_or("warn");

    // Try to build from RUST_LOG, or fall back to a base "warn"
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn"))
        .add_directive(format!("{}={}", pkg_name!(), crate_level).parse().unwrap());

    let fmt_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_level(true)
        .with_thread_ids(args.verbose > 1)
        .with_thread_names(args.verbose > 2);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(env_filter)
        .with(ErrorLayer::default())
        .init();

    Ok(args)
}
