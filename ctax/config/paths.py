from pathlib import Path

# Project root directory path
ROOT = Path(__file__).resolve().parent.parent.parent

# Folder names
DATA_DIR = "data"
RAW_DIR = "data/raw"

# File names
CONFIG_FILE = "config.yaml"


def create_unique_file_path(
    file_name: str | Path,
    *,
    directory: str = "",
) -> Path:
    """Loop for changing filepath in case it already exists"""

    base_name = Path(file_name).stem
    file_extension = Path(file_name).suffix
    file_path = ROOT / DATA_DIR / directory / file_name

    i = 1
    while file_path.exists():
        file_path = ROOT / DATA_DIR / directory / \
            Path(base_name + f"_{i}{file_extension}")
        i += 1

    return file_path


if __name__ == "__main__":
    print(ROOT)
    print(create_unique_file_path("bitpanda_all.csv"))
