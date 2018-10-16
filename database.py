from .average_utils import averages_match
from .simpleflock import SimpleFlock
from .json_numpy import NumpyDecoder
from pathlib import Path
import json
try:
    import objectpath
except ImportError:
    pass


def add_average_to_database(average, encoder):
    parallel_average_path = Path('.') / ".parallel_average"
    database_path = parallel_average_path / "database.json"

    with SimpleFlock(str(parallel_average_path / "dblock")):
        with open(database_path, 'r+') as f:
            if database_path.stat().st_size == 0:
                averages = []
            else:
                averages = json.load(f)

            averages = [a for a in averages if not averages_match(a, average)]
            averages.append(average)
            f.seek(0)
            json.dump(averages, f, indent=2, cls=encoder)
            f.truncate()


class Database:
    def __init__(self):
        self.refresh()

    def refresh(self):
        database_path = Path('.') / ".parallel_average" / "database.json"
        if not database_path.exists() or database_path.stat().st_size == 0:
            self.db = None
            return

        with database_path.open() as f:
            self.db = json.load(f, cls=NumpyDecoder)

    @property
    def function_names(self):
        if self.db is None:
            return []

        return sorted(list({average["function_name"] for average in self.db}))

    def query(self, query_string):
        if self.db is None:
            return []

        if "objectpath" not in globals():
            raise ModuleNotFoundError("Please install the `objectpath` library in order to use this function.")

        tree = objectpath.Tree(self.db)
        return list(tree.execute(query_string))


database = Database()
