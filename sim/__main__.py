"""Прогнать все сценарии и напечатать отчёт: `python -m sim`."""

from __future__ import annotations

from sim.scenarios import SCENARIOS


def main() -> None:
    for scenario in SCENARIOS:
        sim, _ = scenario.run()
        print(f"=== {scenario.name} — {scenario.question}")
        print(sim.report())
        print()


if __name__ == "__main__":
    main()
