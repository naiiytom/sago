# Sago

**Terraform for DataOps**

Sago is a high-performance, declarative data reliability framework written in Rust. It aims to bring the principles of Infrastructure as Code (IaC) to Data Operations, ensuring data quality, consistency, and reliability across your data infrastructure.

## Key Features

- **Cross-Modal Diffing**: Efficiently compare data across different storage engines and formats (e.g., Postgres vs. S3 Parquet).
- **Semantic Schema Analysis**: Understands the meaning of your data, not just the types, to detect subtle schema drifts.
- **Statistical Drift Detection**: automatically identifies when data distribution shifts significantly, alerting you to potential quality issues.
- **Declarative Configuration**: Define your data expectations and reliability checks in a simple TOML configuration.
- **High Performance**: Built on Rust, Tokio, and Apache Arrow for blazing fast execution.

## Quickstart

1.  **Install Sago** (Assuming you have Rust installed):
    ```bash
    cargo install --path sago-cli
    ```

2.  **Initialize a Project**:
    `init` scaffolds `Sago.toml` and a `.sago/` state directory in the current
    directory, so create and enter a folder first:
    ```bash
    mkdir my-data-project && cd my-data-project
    sago init my-data-project
    ```

3.  **Configure Connections**:
    Edit `Sago.toml` to define your data sources and the datasets to track.
    ```toml
    [connections.warehouse]
    type = "postgres"
    url  = "postgres://user:pass@localhost/db"

    [targets.users]
    connection = "warehouse"
    identifier = "public.users"
    ```

4.  **Capture a baseline, then check for drift**:
    ```bash
    sago apply          # snapshot live data into the baseline
    sago plan           # report drift since the last apply
    sago explore        # browse snapshots in an interactive TUI
    ```

    You can also compare two sources directly without a baseline:
    ```bash
    sago diff warehouse:public.users archive:users.parquet
    ```

## Architecture

Sago is composed of the following components:

-   **sago-core**: The core engine containing the logic for diffing, schema analysis, and drift detection.
-   **sago-cli**: The command-line interface for interacting with Sago.
-   **sago-sdk**: Rust SDK for building custom integrations and extensions.
-   **sago-proto**: Protocol Buffer definitions for internal communication and plugin interfaces.

## Roadmap

See [ROADMAP.md](./docs/ROADMAP.md) for future plans.

## Technical Specifications

See [TECHNICAL_SPECIFICATIONS.md](./docs/TECHNICAL_SPECIFICATIONS.md) for detailed technical specs.
