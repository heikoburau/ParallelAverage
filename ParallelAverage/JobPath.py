from pathlib import Path
import re


class JobPath:
    def __init__(self, path):
        self.path = Path(path)
        self.path.mkdir(exist_ok=True)
        self.input_path = self / "input"
        self.input_path.mkdir(exist_ok=True)
        self.data_path = self / "data_output"
        self.data_path.mkdir(exist_ok=True)

    @property
    def task_output_files(self):
        return [t for t in self.data_path.iterdir() if str(t).endswith("_task_output.json")]

    @property
    def task_ids(self):
        if not self.data_path.iterdir():
            return None

        return set(int(re.search(r"\d+", task_file.name).group()) for task_file in self.data_path.iterdir())

    def resolve(self):
        return self.path.resolve()

    def iterdir(self):
        return self.path.iterdir()

    def __str__(self):
        return str(self.path)

    def __truediv__(self, sub_path):
        return self.path / sub_path
