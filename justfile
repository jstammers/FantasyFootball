package := "bayesball"

default:
    just --list

setup:
    @uv sync --all-extras

format:
    @ruff format .