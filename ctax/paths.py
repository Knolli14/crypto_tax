from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT / "data"

CONFIG_PATH = ROOT / "ctax" / "config" / "config.yaml"


def convert_extension(
    file: str|Path,
    extension: str
    ) -> Path:
    """ """

    return f"{Path(file).stem}.{extension}"


def create_file_path(
    file_name: str | Path,
    directory: str
    ) -> Path:
    """
    Loop for changing filepath in case it already exists

    :param output_file: name of the file
    :param folder: subfolder in data directory
    """
    base_name = Path(file_name).stem
    file_path = DATA_DIR / directory/ file_name

    i = 1
    while file_path.exists():
        file_path = DATA_DIR / directory / Path(base_name + f"_{i}.{directory}")
        i += 1

    return file_path
