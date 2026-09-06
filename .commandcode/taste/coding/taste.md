# Coding

- For Python projects lacking lint/type tooling, endorsed adding ruff (lint + import sorting) plus mypy with a lenient starter config as part of the work. Confidence: 0.7
- Prefers behavior-preserving refactors: deduplicate and split overly complex functions without changing the public API surface, module paths, or documented architecture (keeps docs/roadmaps referencing the layout valid). Confidence: 0.6
