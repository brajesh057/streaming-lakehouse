# Interview Notes - Real-Time Streaming Lakehouse

## Day 1 - Environment Setup

### Objective

Set up the development environment and initialize version control before
building the data platform.

### Environment

- Windows 11
- Python 3.12.10
- pip 26.2.1
- Docker 29.8.0
- Docker Compose v5.5.1
- Git 2.56.0
- Docker Desktop using WSL2

### What We Verified

Docker Engine is running successfully.

Docker Compose is available for running multiple services together.

Python virtual environments are available through the built-in venv module.

The direct pip.exe command is blocked by Windows Application Control,
but pip works correctly through:

python -m pip

We did not bypass the Windows security policy.

### Why Git Was Initialized First

The project is tracked from the beginning so that the development history
shows how the system was built incrementally.

### Why Docker

The project contains multiple services with different dependencies.
Docker allows these services to run in isolated and reproducible environments.

Docker Compose will allow us to define and manage the multi-container
environment from configuration.

### Initial Architecture

Producer
    |
    v
Kafka
    |
    v
Spark Structured Streaming
    |
    v
Bronze
    |
    v
Silver
    |
    v
Gold
    |
    v
Analytics / Dashboard

### Git Commands Learned

git init
- Initializes a new local Git repository.

git status
- Shows the current working-tree and staging-area state.

git add .
- Stages eligible changes for the next commit.

.gitignore
- Prevents selected files such as secrets, virtual environments,
  caches, logs, and generated data from being tracked.

### Interview Talking Point

I initialized Git before starting development so that each major
implementation stage could be tracked through meaningful commits.
I also containerized the platform because it consists of multiple
services with different dependencies and configurations.

## Problems Encountered

### pip.exe blocked

Problem:
Windows Application Control blocked the standalone pip.exe.

Solution:
Verified that pip is available through:

python -m pip --version

Result:
pip 26.2.1 is working.

Important:
We did not attempt to bypass the Windows security policy.

## Next Steps

1. Create the first Git commit.
2. Connect the repository to GitHub.
3. Design the initial data schema.
4. Create Docker Compose infrastructure.
5. Build the streaming producer.
