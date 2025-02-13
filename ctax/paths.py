from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT / "data"


def convert_extension(
    file: str|Path,
    extension: str
    ) -> Path:
    """ """

    return f"{Path(file).stem}.{extension}"


def create_file_path(
    output_file: str|Path,
    folder: str
    ) -> Path:
    """ Loop for changing filepath in case it already exists"""
    base_name = Path(output_file).stem
    file_path = DATA_DIR / folder/ output_file

    i = 1
    while file_path.exists():
        file_path = DATA_DIR / folder / Path(base_name + f"_{i}.{folder}")
        i += 1

    return file_path
