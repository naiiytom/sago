# Technical Specifications

## Core Technologies
- **Rust**: Language of choice for performance and safety.
- **Tokio**: Asynchronous runtime for efficient I/O handling.
- **Apache Arrow**: Columnar memory format for high-speed data processing.
- **Merkle Trees**: Efficiently verify data integrity and consistency.
- **PSI (Private Set Intersection) Calculation**: Compare datasets without revealing sensitive information (future).
- **sqlx**: Asynchronous SQL toolkit for database interactions.
- **aws-sdk-s3**: AWS SDK for interacting with S3 storage.
- **clap**: Command-line argument parser.
- **ratatui**: TUI (Text User Interface) library for rich terminal interfaces.

## Architecture Overview
Sago is designed as a modular system with a core engine responsible for the heavy lifting. The CLI acts as the primary interface, while the SDK allows developers to integrate Sago's capabilities into their own applications.

### Components
- **sago-core**:
  - `SchemaProvider` trait: Abstract interface for fetching schemas.
  - Diff Engine: logic for comparing datasets.
  - Drift Detector: Statistical analysis module.
- **sago-cli**:
  - `clap`-based CLI for commands like `init`, `plan`, `apply`.
  - `ratatui`-based TUI for interactive exploration.
- **sago-sdk**:
  - Bindings to `sago-core` functionality.
- **sago-proto**:
  - gRPC/Protobuf definitions for potential future microservices or plugin architecture.
