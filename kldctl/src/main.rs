use clap::{Parser, Subcommand};
use kaleidux_common::{Request, Response};
use tokio::net::UnixStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser)]
#[command(
    name = "kldctl",
    version,
    about = "Kaleidux control utility - manage your dynamic wallpapers",
    long_about = r#"
Kaleidux Control Utility (kldctl)
═════════════════════════════════

A CLI tool to control the Kaleidux wallpaper daemon.

All wallpaper configuration (paths, transitions, volume) is done through
the config file at ~/.config/kaleidux/config.toml

EXAMPLES:
  kldctl next                               Skip to next wallpaper
  kldctl reload                             Reload config from disk
  kldctl love ~/wallpapers/fav.jpg -m 3.0   3x more likely to appear
  kldctl query                              Show connected outputs
  kldctl pause                              Pause video playback

TRANSITIONS (configured in config.toml):
  fade, cube, angular, ripple, doom, pixelize, crosswarp, 
  directional, dreamy, swirl, heart, burn, circle, random, ...
  (50+ transitions available - see documentation)

CONFIG:
  ~/.config/kaleidux/config.toml
"#,
    after_help = "Use 'kldctl <command> --help' for more info on a specific command."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Daemon socket path (defaults to XDG_RUNTIME_DIR/kaleidux.sock or /tmp/kaleidux-{USER}.sock)
    #[arg(short, long, global = true)]
    socket: Option<String>,
    
    /// Show version information
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    version: Option<bool>,
}

#[derive(Subcommand)]
enum Commands {
    /// Show current daemon status and playback state
    #[command(visible_alias = "st")]
    Status,

    /// Switch to the next wallpaper in the queue
    #[command(visible_alias = "n")]
    Next {
        /// Target output (omit for all)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Switch to the previous wallpaper (if history exists)
    #[command(visible_alias = "p")]
    Prev {
        /// Target output (omit for all)
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Mark a file as "loved" - increases its selection frequency
    /// 
    /// Loved files appear more often based on their multiplier.
    /// A multiplier of 2.0 means 2x more likely to be picked.
    Love {
        /// Path to the file
        path: String,

        /// Frequency multiplier (e.g., 2.0 = 2x more likely)
        #[arg(short, long, default_value = "2.0")]
        multiplier: f32,
    },

    /// Remove a file from the love list (reset to normal frequency)
    Unlove {
        /// Path to the file
        path: String,
    },

    /// List all loved wallpapers with their multipliers
    #[command(visible_alias = "ll")]
    Lovelist,

    /// Pause video playback (images unaffected)
    Pause,

    /// Resume video playback
    Resume,

    /// Stop the current wallpaper
    Stop,

    /// Query connected outputs and their current wallpaper
    #[command(visible_alias = "q")]
    Query,

    /// Validate configuration file without starting daemon
    #[command(name = "check-config", visible_alias = "cc")]
    CheckConfig,

    /// Reload configuration from disk
    Reload,

    /// Stop the daemon gracefully
    Kill,

    /// Clear wallpaper on output(s) - show black screen
    Clear {
        /// Target output or omit for all
        #[arg(short, long)]
        output: Option<String>,
    },


    /// Manage playlists
    Playlist {
        #[command(subcommand)]
        command: PlaylistSubcommand,
    },

    /// Manage blacklist
    Blacklist {
        #[command(subcommand)]
        command: BlacklistSubcommand,
    },

    /// Show recently played wallpapers
    History {
        /// Target output (omit for default/all)
        #[arg(short, long)]
        output: Option<String>,
    },
}

#[derive(Subcommand)]
enum PlaylistSubcommand {
    /// Create a new playlist
    Create { name: String },
    /// Delete a playlist
    Delete { name: String },
    /// Add a file to a playlist
    Add { name: String, path: String },
    /// Remove a file from a playlist
    Remove { name: String, path: String },
    /// Load/Activate a playlist (omit name to reset/unload)
    Load { name: Option<String> },
    /// List all playlists
    List,
}

#[derive(Subcommand)]
enum BlacklistSubcommand {
    /// Add a file to the blacklist
    Add { path: String },
    /// Remove a file from the blacklist
    Remove { path: String },
    /// List blacklisted files
    List,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Handle local commands first (don't need daemon connection)
    match &cli.command {
        Commands::CheckConfig => {
            // Validate configuration without connecting to daemon
            let config_path = dirs::config_dir()
                .map(|p| p.join("kaleidux").join("config.toml"))
                .unwrap_or_else(|| std::path::PathBuf::from("config.toml"));
            
            if !config_path.exists() {
                println!("✓ No config file found at {:?} (using defaults)", config_path);
                return Ok(());
            }
            
            let content = std::fs::read_to_string(&config_path)?;
            match toml::from_str::<toml::Value>(&content) {
                Ok(_) => println!("✓ Configuration valid: {:?}", config_path),
                Err(e) => {
                    eprintln!("✗ Configuration error: {}", e);
                    std::process::exit(1);
                }
            }
            return Ok(());
        }
        _ => {}
    }

    let request = match cli.command {
        Commands::Status => Request::QueryOutputs,
        Commands::Next { output } => Request::Next { output },
        Commands::Prev { output } => Request::Prev { output },
        Commands::Love { path, multiplier } => Request::Love { path, multiplier },
        Commands::Unlove { path } => Request::Unlove { path },
        Commands::Lovelist => Request::LoveitList,
        Commands::Pause => Request::Pause,
        Commands::Resume => Request::Resume,
        Commands::Stop => Request::Stop,
        Commands::Query => Request::QueryOutputs,
        Commands::Reload => Request::Reload,
        Commands::Kill => Request::Kill,
        Commands::Clear { output } => Request::Clear { output },

        Commands::CheckConfig => unreachable!(),
        Commands::Playlist { command } => Request::Playlist(match command {
            PlaylistSubcommand::Create { name } => kaleidux_common::PlaylistCommand::Create { name },
            PlaylistSubcommand::Delete { name } => kaleidux_common::PlaylistCommand::Delete { name },
            PlaylistSubcommand::Add { name, path } => kaleidux_common::PlaylistCommand::Add { name, path },
            PlaylistSubcommand::Remove { name, path } => kaleidux_common::PlaylistCommand::Remove { name, path },
            PlaylistSubcommand::Load { name } => kaleidux_common::PlaylistCommand::Load { name },
            PlaylistSubcommand::List => kaleidux_common::PlaylistCommand::List,
        }),
        Commands::Blacklist { command } => Request::Blacklist(match command {
            BlacklistSubcommand::Add { path } => kaleidux_common::BlacklistCommand::Add { path },
            BlacklistSubcommand::Remove { path } => kaleidux_common::BlacklistCommand::Remove { path },
            BlacklistSubcommand::List => kaleidux_common::BlacklistCommand::List,
        }),
        Commands::History { output } => Request::History { output },
    };

    // Determine socket path (use provided or default)
    let socket_path = cli.socket.unwrap_or_else(|| {
        dirs::runtime_dir()
            .map(|d| d.join("kaleidux.sock").to_string_lossy().to_string())
            .unwrap_or_else(|| {
                let uid = std::env::var("USER").unwrap_or_else(|_| "kaleidux".to_string());
                format!("/tmp/kaleidux-{}.sock", uid)
            })
    });
    
    // Connect to daemon
    match UnixStream::connect(&socket_path).await {
        Ok(mut stream) => {
            let req_json = serde_json::to_string(&request)?;
            stream.write_all(req_json.as_bytes()).await?;
            stream.write_all(b"\n").await?;
            
            // Read response
            let mut response = String::new();
            stream.read_to_string(&mut response).await?;
            
            if !response.is_empty() {
                // Try to parse as Response to pretty print if it's a list
                if let Ok(resp) = serde_json::from_str::<Response>(&response) {
                    match resp {
                        Response::LoveitList(entries) => {
                            println!("{:<50} | {:<5} | {:<5}", "Path", "Loveit", "Uses");
                            println!("{}", "-".repeat(66));
                            for entry in entries {
                                println!("{:<50} | {:<5.1} | {:<5}", 
                                    entry.path, entry.multiplier, entry.count);
                            }
                        }
                        Response::OutputInfo(outputs) => {
                            println!("{:<10} | {:<10} | {:<30}", "Output", "Size", "Current Wallpaper");
                            println!("{}", "-".repeat(56));
                            for out in outputs {
                                println!("{:<10} | {}x{} | {:<30}", 
                                    out.name, out.width, out.height, 
                                    out.current_wallpaper.unwrap_or_else(|| "none".to_string()));
                            }
                        }
                        Response::Error(e) => eprintln!("Error: {}", e),
                        Response::Ok => println!("OK"),
                        Response::Playlists(names) => {
                            println!("Playlists:");
                            for name in names {
                                println!(" - {}", name);
                            }
                        }
                        Response::Blacklist(paths) => {
                            println!("Blacklisted Files:");
                            for path in paths {
                                println!(" - {}", path);

                            }
                        }
                        Response::History(paths) => {
                            println!("History (most recent last):");
                            for (i, path) in paths.iter().enumerate() {
                                println!(" {:>2}. {}", i + 1, path);
                            }
                        }
                    }
                } else {
                    println!("{}", response);
                }
            } else {
                println!("OK");
            }
        }
        Err(e) => {
            eprintln!("Failed to connect to daemon at {}: {}", socket_path, e);
            eprintln!("Is kaleidux-daemon running?");
            std::process::exit(1);
        }
    }

    Ok(())
}
