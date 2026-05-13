mod artifact;
mod cli;
mod cli_app_server_client;
mod daemon;
mod fs_layout;
mod models;
#[allow(dead_code)]
mod plugin_rpc;
mod self_update;
mod store;

use anyhow::Result;

pub fn run() -> Result<()> {
    cli::run()
}
