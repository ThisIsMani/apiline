# APIline

**Interactive CLI tool for executing API workflows step-by-step**

APIline is a powerful command-line tool that allows you to define and execute API workflows interactively. Perfect for API testing, workflow automation, and step-by-step debugging of complex API sequences.

## Features

- **Step-by-step execution** - Execute API calls one at a time with confirmation
- **Variable management** - Set and reuse variables across requests
- **Response chaining** - Extract values from responses and use in subsequent requests
- **Interactive controls** - Review, skip, or retry individual steps
- **Bulk execution** - Run all remaining steps without confirmation
- **Hot reload** - Config automatically reloads when the YAML file changes
- **YAML configuration** - Version-controllable workflow definitions
- **Multiple auth types** - Support for API keys, JWT tokens, and custom auth

## Installation

Build from source:

```bash
git clone https://github.com/thisismani/apiline
cd apiline
cargo build --release
```

The binary will be available at `target/release/apiline`.

Optionally, install to your local cargo bin:
```bash
cargo install --path .
```

## Quick Start

1. Create a workflow configuration file:

```yaml
# example-workflow.yaml
variables:
  base_url: "https://api.example.com"
  api_key: "your-api-key"

requests:
  - name: "Get user token"
    method: "POST"
    endpoint: "/auth/login"
    auth: "none"
    payload:
      email: "user@example.com"
      password: "password"
    save_as: "token"
    extract_path: "$.access_token"

  - name: "Get user profile"
    method: "GET"
    endpoint: "/user/profile"
    auth: "jwt"
    expected_status: 200
```

2. Run APIline:

```bash
apiline example-workflow.yaml --base-url https://api.example.com
```

3. Follow the interactive prompts to execute your workflow step by step!

## Configuration Format

APIline uses YAML configuration files with the following structure:

### Variables
```yaml
variables:
  api_key: "your-api-key"
  user_id: "12345"
  environment: "staging"
```

Variables can be:
- Set in the config file
- Updated interactively during execution
- Used in requests with `${variable_name}` syntax

### Requests
```yaml
requests:
  - name: "Human-readable description"
    method: "GET|POST|PUT|DELETE|PATCH"
    endpoint: "/api/endpoint"
    auth: "admin|jwt|none|Bearer token|api-key:value"
    expected_status: 200  # Optional, defaults to 200
    payload:              # Optional, for POST/PUT requests
      key: "value"
      user_id: "${user_id}"
    save_as: "variable_name"      # Optional, save entire response
    extract_path: "$.field_name"  # Optional, extract specific field
    save_multiple:                # Optional, save multiple fields
      token: "$.access_token"
      user_id: "$.user.id"
```

## Usage

### Basic Usage
```bash
# Run a workflow
apiline config.yaml

# Specify base URL
apiline config.yaml --base-url https://api.example.com

# Start from a specific step
apiline config.yaml --start-from 3

# Provide default API key
apiline config.yaml --api-key your-default-key
```

### Interactive Commands

Once APIline starts, you can use these commands:

- **`v` or `vars`** - Show all current variables
- **`s` or `set`** - Set or update a variable value
- **`l` or `list`** - List all requests with their status
- **`n` or `next`** - Execute the next request
- **`a` or `all`** - Execute all remaining requests
- **`1-N`** - Execute a specific step number (e.g., `3`)
- **`q` or `quit`** - Exit the program

## Authentication Types

- **`admin`** - Uses the `--api-key` flag or prompts for API key
- **`jwt`** - Uses a saved JWT token (typically from a previous login request)
- **`none`** - No authentication
- **`Bearer <token>`** - Custom bearer token
- **`api-key:<value>`** - Custom API key

## Variable Substitution

Use variables in your requests with `${variable_name}` syntax:

```yaml
requests:
  - name: "Get user"
    endpoint: "/users/${user_id}"
    payload:
      api_key: "${api_key}"
      environment: "${environment}"
```

## Response Extraction

Extract values from JSON responses:

```yaml
# Save entire response
save_as: "login_response"

# Extract a single field
save_as: "token"
extract_path: "$.access_token"

# Extract multiple fields
save_multiple:
  token: "$.access_token"
  user_id: "$.user.id"
  expires_at: "$.expires_at"
```

## Variable Persistence

APIline automatically saves extracted variables back to your YAML config file:

- When a request extracts values using `save_as` or `save_multiple`, they're immediately written to the `variables` section
- Variables persist across sessions - restart APIline and your tokens/IDs are still there
- Hot reload preserves runtime variables while updating request definitions
- Perfect for long-running workflows where you need to resume later

**Example workflow:**
1. Run login request → JWT token extracted and saved to YAML
2. Edit config file to add new request
3. APIline auto-reloads, token still available
4. Continue with authenticated requests without re-logging in

## Examples

### API Testing Workflow
```yaml
variables:
  base_url: "https://jsonplaceholder.typicode.com"

requests:
  - name: "Get all posts"
    method: "GET"
    endpoint: "/posts"
    auth: "none"

  - name: "Get specific post"
    method: "GET"
    endpoint: "/posts/1"
    auth: "none"

  - name: "Create new post"
    method: "POST"
    endpoint: "/posts"
    auth: "none"
    payload:
      title: "Test Post"
      body: "This is a test post"
      userId: 1
    expected_status: 201
```

### Authentication Flow
```yaml
variables:
  email: "user@example.com"
  password: "password"

requests:
  - name: "Login"
    method: "POST"
    endpoint: "/auth/login"
    auth: "none"
    payload:
      email: "${email}"
      password: "${password}"
    save_as: "jwt_token"
    extract_path: "$.token"

  - name: "Get profile"
    method: "GET"
    endpoint: "/user/profile"
    auth: "jwt"

  - name: "Update profile"
    method: "PUT"
    endpoint: "/user/profile"
    auth: "jwt"
    payload:
      name: "Updated Name"
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache 2.0 License - see the LICENSE file for details.