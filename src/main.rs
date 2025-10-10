use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use notify::{Watcher, RecursiveMode, Event, event::EventKind};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;

#[derive(Parser)]
#[command(name = "apiline")]
#[command(about = "Interactive CLI tool for executing API workflows step-by-step")]
#[command(version = "0.1.0")]
struct Args {
    /// Configuration file path
    config: PathBuf,

    /// Server base URL
    #[arg(long, default_value = "http://localhost:8080")]
    base_url: String,

    /// Default API key for admin authentication
    #[arg(long, default_value = "")]
    api_key: String,

    /// Start from specific step number
    #[arg(long)]
    start_from: Option<usize>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct ApilineConfig {
    #[serde(default)]
    variables: HashMap<String, String>,
    requests: Vec<ApiRequest>,
}

#[derive(Debug, Deserialize, serde::Serialize, Clone)]
struct ApiRequest {
    name: String,
    method: String,
    endpoint: String,
    payload: Option<serde_json::Value>,
    auth: String,
    #[serde(default = "default_status")]
    expected_status: u16,
    #[serde(default)]
    save_as: Option<String>,
    #[serde(default)]
    extract_path: Option<String>,
    #[serde(default)]
    save_multiple: Option<HashMap<String, String>>,
}

fn default_status() -> u16 {
    200
}

fn load_config(config_path: &Path) -> Result<ApilineConfig> {
    let config_content = std::fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config: {:?}", config_path))?;

    serde_yaml::from_str(&config_content).context("Failed to parse YAML config")
}

fn save_config(config_path: &Path, config: &ApilineConfig) -> Result<()> {
    let yaml_content = serde_yaml::to_string(config)
        .context("Failed to serialize config to YAML")?;

    std::fs::write(config_path, yaml_content)
        .with_context(|| format!("Failed to write config to {:?}", config_path))?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Load initial config
    let mut config = load_config(&args.config)?;

    // Set up file watcher for hot reloading
    let (tx, rx) = channel();
    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            if matches!(event.kind, EventKind::Modify(_)) {
                let _ = tx.send(());
            }
        }
    }).context("Failed to create file watcher")?;

    watcher.watch(&args.config, RecursiveMode::NonRecursive)
        .context("Failed to watch config file")?;

    let client = Client::new();
    let base_url = args.base_url;
    let default_api_key = args.api_key;

    println!("{}", "🚀 APIline - Interactive API Workflow Tool".bold().blue());
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("{}", format!("📁 Watching: {:?}", args.config).dimmed());
    println!("{}", "💡 Config will auto-reload on file changes".dimmed());

    let mut current_step = args.start_from.unwrap_or(0);

    loop {
        // Check for config file changes (non-blocking)
        if let Ok(()) = rx.try_recv() {
            println!("\n{}", "🔄 Config file changed, reloading...".yellow());

            match load_config(&args.config) {
                Ok(new_config) => {
                    // Preserve runtime variables
                    let old_variables = config.variables.clone();
                    config = new_config;

                    // Merge old runtime variables with new config
                    for (key, value) in old_variables {
                        config.variables.entry(key).or_insert(value);
                    }

                    println!("{}", "✅ Config reloaded successfully!".green());
                    println!("{}", "   Variables from previous session preserved".dimmed());
                }
                Err(e) => {
                    println!("{}", format!("❌ Failed to reload config: {}", e).red());
                    println!("{}", "   Continuing with previous config".yellow());
                }
            }
        }
        // Show menu
        show_menu(&config, current_step)?;

        // Get user input
        print!("\n{} ", "Choose option:".bold());
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let choice = input.trim();

        match choice {
            "v" | "vars" => {
                show_variables(&config.variables);
            }
            "l" | "list" => {
                list_requests(&config.requests, current_step);
            }
            "s" | "set" => {
                set_variable(&mut config.variables)?;
            }
            "q" | "quit" => {
                println!("{}", "Goodbye! 👋".green());
                break;
            }
            "n" | "next" => {
                if current_step < config.requests.len() {
                    match execute_request_with_option(
                        &client,
                        &base_url,
                        &default_api_key,
                        &mut config,
                        &args.config,
                        current_step,
                        false,
                    )
                    .await
                    {
                        Ok(true) => {
                            current_step += 1;
                            if current_step >= config.requests.len() {
                                println!("\n{}", "✅ All requests completed!".bold().green());
                            }
                        }
                        Ok(false) => {
                            println!("{}", "Request cancelled".yellow());
                        }
                        Err(e) => {
                            println!("{} {}", "❌ Error:".red(), e);
                        }
                    }
                } else {
                    println!("{}", "No more requests to execute".yellow());
                }
            }
            "a" | "all" => {
                print!(
                    "{} [Y/n]: ",
                    "Execute all remaining requests without confirmation?".bold()
                );
                io::stdout().flush()?;

                let mut confirm_all = String::new();
                io::stdin().read_line(&mut confirm_all)?;
                let skip_confirmations = confirm_all.trim().to_lowercase() != "n"
                    && confirm_all.trim().to_lowercase() != "no";

                println!("{}", "Executing all remaining requests...".blue());
                while current_step < config.requests.len() {
                    println!(
                        "\n{}",
                        format!("Step {}/{}", current_step + 1, config.requests.len()).bold()
                    );
                    match execute_request_with_option(
                        &client,
                        &base_url,
                        &default_api_key,
                        &mut config,
                        &args.config,
                        current_step,
                        skip_confirmations,
                    )
                    .await
                    {
                        Ok(true) => {
                            current_step += 1;
                        }
                        Ok(false) => {
                            println!("{}", "Request skipped".yellow());
                            current_step += 1;
                        }
                        Err(e) => {
                            println!("{} {}", "❌ Error:".red(), e);
                            println!(
                                "{}",
                                "Stopping execution. Use 'n' to continue from here.".yellow()
                            );
                            break;
                        }
                    }
                }
                if current_step >= config.requests.len() {
                    println!("\n{}", "✅ All requests completed!".bold().green());
                }
            }
            _ => {
                // Try to parse as step number
                if let Ok(step_num) = choice.parse::<usize>() {
                    if step_num > 0 && step_num <= config.requests.len() {
                        let step_index = step_num - 1;
                        match execute_request_with_option(
                            &client,
                            &base_url,
                            &default_api_key,
                            &mut config,
                            &args.config,
                            step_index,
                            false,
                        )
                        .await
                        {
                            Ok(true) => {
                                println!("{}", "✅ Request completed successfully".green());
                                // Update current step if we executed the next one
                                if step_index == current_step {
                                    current_step += 1;
                                }
                            }
                            Ok(false) => {
                                println!("{}", "Request cancelled".yellow());
                            }
                            Err(e) => {
                                println!("{} {}", "❌ Error:".red(), e);
                            }
                        }
                    } else {
                        println!("{}", "Invalid step number".red());
                    }
                } else {
                    println!(
                        "{}",
                        "Invalid option. Try 'v', 's', 'l', 'n', 'a', or a step number.".red()
                    );
                }
            }
        }
    }

    Ok(())
}

fn set_variable(variables: &mut HashMap<String, String>) -> Result<()> {
    print!("{} ", "Variable name:".bold());
    io::stdout().flush()?;

    let mut var_name = String::new();
    io::stdin().read_line(&mut var_name)?;
    let var_name = var_name.trim().to_string();

    if var_name.is_empty() {
        println!("{}", "Variable name cannot be empty".red());
        return Ok(());
    }

    // Show current value if exists
    if let Some(current_value) = variables.get(&var_name) {
        println!("Current value: {}", current_value.cyan());
    }

    print!("{} ", "New value:".bold());
    io::stdout().flush()?;

    let mut var_value = String::new();
    io::stdin().read_line(&mut var_value)?;
    let var_value = var_value.trim().to_string();

    variables.insert(var_name.clone(), var_value.clone());
    println!("✅ Set {}: {}", var_name.yellow(), var_value.green());

    Ok(())
}

fn show_menu(config: &ApilineConfig, current_step: usize) -> Result<()> {
    println!("\n{}", "📋 Menu Options:".bold().cyan());
    println!("  {} - Show all variables", "v".bold().yellow());
    println!("  {} - Set/update variable", "s".bold().yellow());
    println!("  {} - List all requests", "l".bold().yellow());
    println!("  {} - Execute next request", "n".bold().green());
    println!("  {} - Execute all remaining", "a".bold().green());
    println!(
        "  {} - Execute specific step (e.g., '3')",
        "1-N".bold().blue()
    );
    println!("  {} - Quit", "q".bold().red());

    if current_step < config.requests.len() {
        let next_request = &config.requests[current_step];
        println!(
            "\n{} {}: {}",
            "Next:".bold(),
            format!("Step {}", current_step + 1).blue(),
            next_request.name.green()
        );
    } else {
        println!("\n{}", "All requests completed ✅".green());
    }

    Ok(())
}

fn show_variables(variables: &HashMap<String, String>) {
    println!("\n{}", "📊 Current Variables:".bold().cyan());
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    if variables.is_empty() {
        println!("  {}", "No variables set".dimmed());
        return;
    }

    for (key, value) in variables {
        let display_value = if value.is_empty() {
            "<empty>".dimmed().to_string()
        } else if value.len() > 60 {
            format!("{}...", &value[..57])
        } else {
            value.clone()
        };

        println!("  {}: {}", key.yellow(), display_value.cyan());
    }
}

fn list_requests(requests: &[ApiRequest], current_step: usize) {
    println!("\n{}", "📝 Available Requests:".bold().cyan());
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for (i, request) in requests.iter().enumerate() {
        let status = if i < current_step {
            "✅".green()
        } else if i == current_step {
            "➡️".blue()
        } else {
            "⏳".dimmed()
        };

        println!(
            "  {} {}: {} {} {}",
            status,
            format!("{:2}", i + 1).bold(),
            request.method.to_uppercase().magenta(),
            request.endpoint.cyan(),
            request.name.green()
        );
    }
}

async fn execute_request_with_option(
    client: &Client,
    base_url: &str,
    default_api_key: &str,
    config: &mut ApilineConfig,
    config_path: &Path,
    step_index: usize,
    skip_confirmation: bool,
) -> Result<bool> {
    let request = config.requests[step_index].clone();

    println!(
        "\n{} {}: {}",
        "🔄 Preparing".bold(),
        format!("Step {}", step_index + 1).blue(),
        request.name.green()
    );
    println!(
        "   {} {}",
        request.method.to_uppercase().magenta(),
        request.endpoint.cyan()
    );

    // Substitute variables in payload
    let payload = if let Some(mut payload) = request.payload.clone() {
        substitute_variables(&mut payload, &config.variables)?;
        Some(payload)
    } else {
        None
    };

    // Show request preview
    println!("\n{}", "📋 Request Preview:".bold().yellow());
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Method: {}", request.method.to_uppercase().magenta());
    println!("URL: {}{}", base_url.cyan(), request.endpoint.cyan());
    println!("Auth: {}", request.auth.blue());

    if let Some(ref payload) = payload {
        println!("Payload:");
        println!(
            "{}",
            serde_json::to_string_pretty(payload)
                .unwrap_or_default()
                .cyan()
        );
    } else {
        println!("Payload: {}", "None".dimmed());
    }

    // Ask for confirmation (unless skipped)
    if !skip_confirmation {
        print!("\n{} [Y/n]: ", "Execute this request?".bold());
        io::stdout().flush()?;

        let mut confirm = String::new();
        io::stdin().read_line(&mut confirm)?;
        let confirm = confirm.trim().to_lowercase();

        if confirm == "n" || confirm == "no" {
            println!("{}", "❌ Request cancelled".yellow());
            return Ok(false);
        }
    }

    // Make API call
    let response = make_api_call(
        client,
        base_url,
        default_api_key,
        &config.variables.get("jwt_token").unwrap_or(&String::new()),
        &request,
        payload,
    )
    .await?;

    // Save response values
    let mut variables_updated = false;

    if let (Some(save_as), Some(extract_path)) = (&request.save_as, &request.extract_path) {
        if let Some(value) = extract_json_path(&response, extract_path)? {
            config.variables.insert(save_as.clone(), value.clone());
            println!("   💾 Saved {}: {}", save_as.yellow(), value.green());
            variables_updated = true;
        }
    }

    if let Some(save_multiple) = &request.save_multiple {
        for (var_name, extract_path) in save_multiple {
            if let Some(value) = extract_json_path(&response, extract_path)? {
                config.variables.insert(var_name.clone(), value.clone());
                println!("   💾 Saved {}: {}", var_name.yellow(), value.green());
                variables_updated = true;
            }
        }
    }

    // Persist variables to config file
    if variables_updated {
        match save_config(config_path, config) {
            Ok(()) => {
                println!("{}", "   📝 Variables saved to config file".dimmed());
            }
            Err(e) => {
                println!("   {}  {}", "⚠️  Warning: Failed to save config:".yellow(), e);
            }
        }
    }

    Ok(true)
}

fn substitute_variables(
    value: &mut serde_json::Value,
    variables: &HashMap<String, String>,
) -> Result<()> {
    match value {
        serde_json::Value::String(s) => {
            for (var_name, var_value) in variables {
                let placeholder = format!("${{{}}}", var_name);
                if s.contains(&placeholder) {
                    *s = s.replace(&placeholder, var_value);
                }
            }
        }
        serde_json::Value::Object(map) => {
            for (_, v) in map.iter_mut() {
                substitute_variables(v, variables)?;
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr.iter_mut() {
                substitute_variables(item, variables)?;
            }
        }
        _ => {}
    }
    Ok(())
}

async fn make_api_call(
    client: &Client,
    base_url: &str,
    default_api_key: &str,
    jwt_token: &str,
    request: &ApiRequest,
    payload: Option<serde_json::Value>,
) -> Result<serde_json::Value> {
    let url = format!("{}{}", base_url, request.endpoint);

    let method = match request.method.to_uppercase().as_str() {
        "GET" => reqwest::Method::GET,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        "PATCH" => reqwest::Method::PATCH,
        _ => return Err(anyhow::anyhow!("Unsupported method: {}", request.method)),
    };

    let mut req = client
        .request(method, &url)
        .header("Content-Type", "application/json");

    match request.auth.as_str() {
        "admin" => {
            req = req.header("api-key", default_api_key);
        }
        "jwt" => {
            req = req.header("Authorization", format!("Bearer {}", jwt_token));
        }
        "none" => {
            // No authentication
        }
        custom_auth if custom_auth.starts_with("Bearer ") => {
            req = req.header("Authorization", custom_auth);
        }
        custom_auth if custom_auth.starts_with("api-key:") => {
            req = req.header("api-key", custom_auth.strip_prefix("api-key:").unwrap());
        }
        _ => return Err(anyhow::anyhow!("Unknown auth type: {}", request.auth)),
    }

    if let Some(payload) = payload {
        req = req.json(&payload);
    }

    let response = req.send().await.context("Failed to send request")?;

    let status = response.status();
    let response_text = response.text().await.context("Failed to read response")?;

    println!(
        "   📥 Response: {} {}",
        status
            .as_u16()
            .to_string()
            .if_else(status.is_success(), |s| s.green(), |s| s.red()),
        if response_text.len() > 100 {
            format!("{}...", &response_text[..97])
        } else {
            response_text.clone()
        }
        .dimmed()
    );

    if status.as_u16() != request.expected_status {
        return Err(anyhow::anyhow!(
            "Expected status {}, got {}: {}",
            request.expected_status,
            status,
            response_text
        ));
    }

    // Handle empty responses
    if response_text.trim().is_empty() {
        return Ok(serde_json::Value::Object(serde_json::Map::new()));
    }

    serde_json::from_str(&response_text)
        .with_context(|| format!("Failed to parse JSON response: {}", response_text))
}

fn extract_json_path(response: &serde_json::Value, path: &str) -> Result<Option<String>> {
    if path.starts_with("$.") {
        let field = &path[2..];
        if let Some(value) = response.get(field) {
            return Ok(Some(
                value.as_str().unwrap_or(&value.to_string()).to_string(),
            ));
        }
    }
    Ok(None)
}

trait ColoredExt {
    fn if_else<F1, F2>(self, condition: bool, true_fn: F1, false_fn: F2) -> colored::ColoredString
    where
        F1: FnOnce(colored::ColoredString) -> colored::ColoredString,
        F2: FnOnce(colored::ColoredString) -> colored::ColoredString;
}

impl ColoredExt for colored::ColoredString {
    fn if_else<F1, F2>(self, condition: bool, true_fn: F1, false_fn: F2) -> colored::ColoredString
    where
        F1: FnOnce(colored::ColoredString) -> colored::ColoredString,
        F2: FnOnce(colored::ColoredString) -> colored::ColoredString,
    {
        if condition {
            true_fn(self)
        } else {
            false_fn(self)
        }
    }
}

impl ColoredExt for String {
    fn if_else<F1, F2>(self, condition: bool, true_fn: F1, false_fn: F2) -> colored::ColoredString
    where
        F1: FnOnce(colored::ColoredString) -> colored::ColoredString,
        F2: FnOnce(colored::ColoredString) -> colored::ColoredString,
    {
        if condition {
            true_fn(self.normal())
        } else {
            false_fn(self.normal())
        }
    }
}
