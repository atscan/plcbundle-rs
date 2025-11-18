// Completions command - generate shell completion scripts
use anyhow::Result;
use clap::{Args, CommandFactory, ValueEnum};
use clap_complete::{Shell, generate};
use plcbundle::constants;
use std::io;

#[derive(Args)]
#[command(
    alias = "complete",
    alias = "completion",
    alias = "autocomplete",
    about = "Generate shell completion scripts",
    long_about = "Generate shell completion scripts for various shells to enable tab completion
for commands and arguments.

This command outputs completion scripts that can be sourced or installed in your shell
configuration to enable intelligent tab completion. Supported shells include bash, zsh,
fish, and PowerShell.

After generating the script, you need to source it or install it in the appropriate
location for your shell. The command provides instructions for each shell type.

Examples:
  # Generate bash completion
  {bin} completions bash > ~/.bash_completion.d/{bin}

  # Generate zsh completion
  {bin} completions zsh > ~/.zsh/completions/_{bin}

  # Generate fish completion
  {bin} completions fish > ~/.config/fish/completions/{bin}.fish

  # Generate PowerShell completion
  {bin} completions powershell > {bin}.ps1",
    help_template = crate::clap_help!(
        examples: "  # Generate bash completion script\n  \
                   {bin} completions bash > ~/.bash_completion.d/plcbundle\n  \
                   echo 'source ~/.bash_completion.d/plcbundle' >> ~/.bashrc\n\n  \
                   # Generate zsh completion script\n  \
                   {bin} completions zsh > ~/.zsh/completions/_plcbundle\n  \
                   # Add to ~/.zshrc: fpath=(~/.zsh/completions $fpath)\n\n  \
                   # Generate fish completion script\n  \
                   {bin} completions fish > ~/.config/fish/completions/plcbundle.fish\n\n  \
                   # Generate PowerShell completion script\n  \
                   {bin} completions powershell > plcbundle.ps1\n  \
                   # Then: . ./plcbundle.ps1"
    )
)]
pub struct CompletionsCommand {
    /// Shell to generate completions for
    /// If not specified, shows installation instructions
    #[arg(value_enum)]
    pub shell: Option<ShellArg>,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum ShellArg {
    /// Bash completion script
    Bash,
    /// Zsh completion script
    Zsh,
    /// Fish completion script
    Fish,
    /// PowerShell completion script
    #[value(name = "powershell")]
    PowerShell,
}

impl From<ShellArg> for Shell {
    fn from(shell: ShellArg) -> Self {
        match shell {
            ShellArg::Bash => Shell::Bash,
            ShellArg::Zsh => Shell::Zsh,
            ShellArg::Fish => Shell::Fish,
            ShellArg::PowerShell => Shell::PowerShell,
        }
    }
}

pub fn run(cmd: CompletionsCommand) -> Result<()> {
    if let Some(shell_arg) = cmd.shell {
        // Generate completion script
        let shell: Shell = shell_arg.into();
        let mut app = super::Cli::command();
        let bin_name = app.get_name().to_string();

        generate(shell, &mut app, bin_name, &mut io::stdout());
    } else {
        // Show instructions
        show_instructions();
    }

    Ok(())
}

fn show_instructions() {
    let bin_name = constants::BINARY_NAME;

    println!("Shell Completion Setup Instructions");
    println!("════════════════════════════════════\n");
    println!(
        "Generate completion scripts for your shell to enable tab completion for {}.\n",
        bin_name
    );
    println!("Usage:");
    println!("  {} completions <SHELL>\n", bin_name);
    println!("Supported shells:");
    println!("  bash       - Bash completion script");
    println!("  zsh        - Zsh completion script");
    println!("  fish       - Fish completion script");
    println!("  powershell - PowerShell completion script\n");
    println!("Examples:\n");

    // Bash
    println!("Bash:");
    println!("  # Generate completion script");
    println!(
        "  {} completions bash > ~/.bash_completion.d/{}",
        bin_name, bin_name
    );
    println!("  # Add to ~/.bashrc:");
    println!(
        "  echo 'source ~/.bash_completion.d/{}' >> ~/.bashrc",
        bin_name
    );
    println!("  source ~/.bashrc\n");

    // Zsh
    println!("Zsh:");
    println!("  # Generate completion script");
    println!("  mkdir -p ~/.zsh/completions");
    println!(
        "  {} completions zsh > ~/.zsh/completions/_{}",
        bin_name, bin_name
    );
    println!("  # Add to ~/.zshrc:");
    println!("  echo 'fpath=(~/.zsh/completions $fpath)' >> ~/.zshrc");
    println!("  echo 'autoload -U compinit && compinit' >> ~/.zshrc");
    println!("  source ~/.zshrc\n");

    // Fish
    println!("Fish:");
    println!("  # Generate completion script");
    println!("  mkdir -p ~/.config/fish/completions");
    println!(
        "  {} completions fish > ~/.config/fish/completions/{}.fish",
        bin_name, bin_name
    );
    println!("  # Fish will automatically load completions from this directory\n");

    // PowerShell
    println!("PowerShell:");
    println!("  # Generate completion script");
    println!("  {} completions powershell > {}.ps1", bin_name, bin_name);
    println!("  # Load in PowerShell:");
    println!("  . ./{}.ps1", bin_name);
    println!("  # Or add to your PowerShell profile:");
    println!("  . $PROFILE  # if it exists, or create it");
    println!("  echo '. ./{}.ps1' >> $PROFILE\n", bin_name);

    println!("After installation, restart your shell or source the configuration file.");
    println!(
        "Then you can use tab completion for {} commands and arguments!",
        bin_name
    );
}
