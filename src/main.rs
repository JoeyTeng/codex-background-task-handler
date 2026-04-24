fn main() {
    if let Err(error) = codex_background_task_handler::run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
