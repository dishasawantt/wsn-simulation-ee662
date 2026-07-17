# WSN Simulation (EE662)

> Wireless sensor network discrete-event simulation coursework — cluster formation, routing, failure recovery, and energy modeling.

## Contents

| Directory | Description |
|---|---|
| `wsnlab/` | Assignment 2 — self-organizing WSN with SimPy simulator and Tk visualization |
| `wsnsimpy/` | Base SimPy WSN library |
| `final-project/` | EE662 final project parts 1–8 (incremental protocol implementations) |
| `img/` | Example simulation screenshots |

## Prerequisites

```bash
pip install simpy
```

## Run Assignment 2

```bash
cd wsnlab
python run_complete_simulation.py
```

## Run Final Project Examples

```bash
cd final-project
python example_p1.py
```

Final project scripts import from `wsnlab/source/` — run from repo root or ensure `source/` is on `PYTHONPATH`.

## Technology

- Python 3
- SimPy (discrete-event simulation)
- Tkinter (topology visualization)

## Author

**Disha Sawant** — [GitHub](https://github.com/dishasawantt)
