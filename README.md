# Quantitative Research - Stage 1 Skeleton

## Overview
Headless execution environment for Python-based quantitative backtesting.

**Current Stage:** Infrastructure only (no trading logic)

## Quick Start

```bash
# Setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run
python run_stage1.py configs/default.yaml
```

## Structure

```
quant-research/
├── src/              # Source code
├── configs/          # YAML configurations
├── data/             # Market data
├── results/          # Timestamped outputs
└── logs/             # Execution logs
```

## Features

✓ Timestamped output directories  
✓ Dual logging (console + file)  
✓ Configuration snapshots  
✓ Deterministic random seeds  
✓ SSH-persistent execution  

## Linux Execution

**Background (nohup):**
```bash
nohup python run_stage1.py configs/default.yaml > output.log 2>&1 &
```

**Interactive (tmux):**
```bash
tmux new -s quant
python run_stage1.py configs/default.yaml
# Ctrl+B, D to detach
```

## Next Stages

- Stage 2: Trading logic implementation
- Stage 3: Optimization and parameter sweeps
- Stage 4: Reporting and visualization

## Requirements

- Python 3.8+
- Ubuntu/Linux
- SSH access for remote execution
