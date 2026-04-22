import subprocess
import sys


def run_step(module_name: str):
    cmd = [sys.executable, "-m", module_name]
    result = subprocess.run(cmd, check=True)
    return result.returncode


def main():
    run_step("scripts.08_build_star_schema")
    run_step("scripts.09_build_data_marts")
    print("Block 4 vollständig abgeschlossen.")


if __name__ == "__main__":
    main()