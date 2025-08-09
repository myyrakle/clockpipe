use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Command {
    #[clap(subcommand)]
    pub action: SubCommand,
}

#[derive(clap::Subcommand, Debug)]
pub enum SubCommand {
    Run(run::Command),
}

pub mod run {
    use clap::Args;
    use serde::Deserialize;

    #[derive(Clone, Debug, Default, Deserialize, Args)]
    pub struct ConfigOptions {
        #[clap(long, help = "config file path")]
        pub config_file: String,
    }

    #[derive(Clone, Debug, Args)]
    #[clap(name = "run", about = "Run the application")]
    pub struct Command {
        #[clap(flatten)]
        pub value: ConfigOptions,
    }
}
