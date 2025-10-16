# Cross-Platform Installation Guide

This project supports installation and execution on **Linux**, **macOS**, and **Windows** environments.

## Supported Platforms

### ✅ Linux
- **Ubuntu/Debian**: `sudo apt install python3 python3-pip`
- **RHEL/CentOS**: `sudo yum install python3 python3-pip`
- **Arch Linux**: `sudo pacman -S python python-pip`

### ✅ macOS
- **Homebrew**: `brew install python3`
- **Official installer**: Download from [python.org](https://python.org)
- **Xcode tools**: `xcode-select --install` (for compilation)

### ✅ Windows
- **Microsoft Store**: Search for "Python" and install
- **Official installer**: Download from [python.org](https://python.org)
- **Package managers**: `winget install Python.Python.3` or `choco install python`

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/ahmedatef15011/HugEval.git
cd HugEval

# 2. Install dependencies (works on all platforms)
./run install

# 3. Run tests
./run test

# 4. Process model URLs
./run urls.txt
```

## Installation Strategy

The `./run install` command uses a multi-method approach:

1. **Primary**: `pip install --user -e '.[dev]'` (autograder compatible)
2. **Fallback**: `pip install -e '.[dev]'` (for restricted environments)
3. **Recovery**: `pip install --user --force-reinstall -e '.[dev]'`
4. **Last resort**: Upgrade pip and retry

## Platform-Specific Features

### Windows
- Detects Windows environment via `WINDIR`/`SYSTEMROOT`
- Prefers `py` launcher when available
- Provides Administrator privilege suggestions

### macOS
- Suggests Xcode command line tools for compilation
- Recommends Homebrew Python for best compatibility
- Handles both system and Homebrew Python installations

### Linux
- Provides distribution-specific package installation commands
- Suggests build dependencies for compilation
- Handles both system and user-space installations

## Troubleshooting

If installation fails, the script provides platform-specific guidance:

### Common Issues
- **Permission errors**: Try `--user` flag or run with elevated privileges
- **Missing compiler**: Install build tools (Xcode, build-essential, etc.)
- **Old pip version**: Upgrade with `pip install --upgrade pip`

### Environment Variables
- `LOG_FILE`: Specify log output location
- `LOG_LEVEL`: Set logging verbosity (0=silent, 1=info, 2=debug)

## Requirements

- **Python**: 3.8+ (tested on 3.8, 3.9, 3.10, 3.11, 3.12)
- **pip**: Latest version recommended
- **Internet**: Required for Hugging Face API access
