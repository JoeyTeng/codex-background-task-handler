mod artifact;
mod cli;
mod fs_layout;
mod models;
mod store;

use anyhow::Result;

pub fn run() -> Result<()> {
    cli::run()
}
