from .average import Average
from .simpleflock import SimpleFlock
from pathlib import Path
import json


def read_database(path):
    parallel_average_path = Path(path) / ".parallel_average"
    database_path = Path(path) / "parallel_average_database.json"

    with SimpleFlock(str(parallel_average_path / "dblock")):
        with database_path.open() as f:
            averages = json.load(f)

    return [Average(average) for average in averages]


def add_average_to_database(path, average, encoder):
    parallel_average_path = Path(path) / ".parallel_average"
    database_path = Path(path) / "parallel_average_database.json"
    with SimpleFlock(str(parallel_average_path / "dblock")):
        with open(database_path, 'r+') as f:
            if database_path.stat().st_size == 0:
                averages = []
            else:
                averages = json.load(f)

            averages = [Average(average) for average in averages]

            averages = [a for a in averages if a != average]
            averages.append(average)
            f.seek(0)
            json.dump(averages, f, indent=2, cls=encoder)
            f.truncate()
