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

    use crate::errors;

    #[derive(Clone, Debug, Default, Deserialize, Args)]
    pub struct ConfigOptions {
        #[clap(long, help = "config file path")]
        pub config_file: String,
    }

    impl ConfigOptions {
        pub fn read_config_from_file(&self) -> errors::Result<crate::config::Configuraion> {
            log::debug!("Reading configuration from file: {}", self.config_file);

            let config_content = std::fs::read_to_string(&self.config_file)?;

            let parse_result = serde_json::from_str(&config_content);

            match parse_result {
                Ok(config) => {
                    log::info!(
                        "Successfully loaded configuration from {}",
                        self.config_file
                    );
                    Ok(config)
                }
                Err(error) => {
                    return Err(errors::Errors::ConfigReadError(format!(
                        "Failed to parse configuration file: {}",
                        error
                    )));
                }
            }
        }
    }

    #[derive(Clone, Debug, Args)]
    #[clap(name = "run", about = "Run the application")]
    pub struct Command {
        #[clap(flatten)]
        pub value: ConfigOptions,
    }
}
