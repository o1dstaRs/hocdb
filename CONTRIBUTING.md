# Contributing to HOCDB

Thank you for your interest in contributing to HOCDB! We welcome contributions from the community and are excited to work with you to make HOCDB even better.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Pull Request Process](#pull-request-process)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Features](#suggesting-features)
- [Style Guidelines](#style-guidelines)

## Code of Conduct

This project and everyone participating in it is governed by our Code of Conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## How Can I Contribute?

### Reporting Bugs
We welcome bug reports! Please see [Reporting Bugs](#reporting-bugs) for details.

### Suggesting Features
Have an idea for a new feature? See [Suggesting Features](#suggesting-features).

### Submitting Code Changes
- Fix issues and bugs
- Improve documentation
- Add new features
- Enhance performance
- Add tests

## Development Setup

### Prerequisites
- **Zig 0.15.2** - The entire project is written in Zig
- Git
- A modern terminal/shell

### Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/hocdb.git
   cd hocdb
   ```
3. Create a branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```
4. Make your changes
5. Test your changes:
   ```bash
   # Run tests
   zig build test
   
   # Run benchmarks (to ensure no performance regression)
   zig build bench
   ```
6. Commit your changes with a descriptive commit message
7. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

## Coding Standards

### Zig-Specific Guidelines
- Follow Zig's official style guide
- Use descriptive names for functions, variables, and types
- Comment public APIs and complex logic
- Keep functions small and focused
- Write tests for new functionality
- Maintain performance benchmarks

### Project-Specific Guidelines
- HOCDB is designed for high performance - always consider performance implications
- Maintain the append-only, sequential design philosophy
- Preserve the zero-copy memory philosophy in bindings
- Ensure cross-platform compatibility (Zig supports many targets)

## Pull Request Process

1. Ensure your PR addresses a single issue or implements a single feature
2. Update the README.md if your changes introduce new features or modify usage
3. Add tests for new functionality
4. Run the full test suite: `zig build test`
5. Include performance benchmarks if applicable
6. Update the documentation as needed
7. Submit your pull request against the `main` branch
8. Describe your changes in the PR description
9. Link any related issues in the PR description

## Reporting Bugs

### Before Submitting a Bug Report

- Check the [Issues](https://github.com/hocai/hocdb/issues) page to see if the bug has already been reported
- Ensure you are using the latest version of HOCDB
- Verify that the issue is reproducible

### How to Submit a Good Bug Report

Bugs are tracked as [GitHub issues](https://github.com/hocai/hocdb/issues). When reporting a bug, please include:

- **Summary**: A clear, brief description of the problem
- **Environment**: 
  - Zig version
  - Operating system
  - Hardware (CPU, RAM, etc.)
- **Steps to Reproduce**: Detailed steps to reproduce the issue
- **Expected Behavior**: What you expected to happen
- **Actual Behavior**: What actually happened
- **Code Sample**: Minimal code that reproduces the issue
- **Performance Impact**: If applicable, describe the performance impact

Example:

> **Bug: Write performance degrades significantly after X hours of continuous usage**
> 
> Environment: Zig 0.15.2, macOS 14, M2 Pro
> 
> Steps to reproduce:
> 1. Initialize a new database
> 2. Continuously append records for several hours
> 3. Observe write performance over time
> 
> Expected: Consistent performance throughout
> Actual: Performance drops by 50% after ~2 hours
> 
> [Include relevant code snippet]

## Suggesting Features

### Before Submitting a Feature Request

- Check the [Issues](https://github.com/hocai/hocdb/issues) to see if the feature has already been suggested
- Consider if your feature fits the core philosophy of HOCDB: extreme performance for time-series data

### How to Submit a Good Feature Request

Feature requests are tracked as [GitHub issues](https://github.com/hocai/hocdb/issues). When suggesting a feature, please include:

- **Summary**: A clear description of the feature
- **Motivation**: Why this feature is needed
- **Use Cases**: Specific scenarios where this feature would be valuable
- **Proposed Solution**: How you envision the feature working
- **Alternatives**: Other approaches you've considered
- **Performance Considerations**: Impact on performance if applicable

Example:

> **Feature: Support for data compression**
> 
> Motivation: For users with large datasets, disk space efficiency is important while maintaining high performance
> 
> Use Cases: 
> - Long-term storage of time-series data
> - Archiving historical data
> 
> Proposed Solution:
> - Implement optional compression at the file level
> - Support for different compression algorithms (LZ4, ZSTD)
> - Transparent compression/decompression for users
> 
> Performance Considerations: This should be optional to maintain raw speed for users who don't need compression

## Style Guidelines

### Git Commit Messages
- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

### Documentation
- Update documentation when adding new features or changing existing functionality
- Use clear, concise language
- Include examples where helpful
- Follow the existing documentation style

---

## Questions?

If you have questions about contributing, feel free to open an issue with the "question" label or reach out to the maintainers.

Thank you for contributing to HOCDB!