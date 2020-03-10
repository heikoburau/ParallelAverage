"""
This class produces a total result from each individual task result it finds in the 'data_output' folder.
The total result has the same structure as an individual result.
"""


from collections import defaultdict
import json
from .json_numpy import NumpyEncoder, NumpyDecoder
from .Dataset import Dataset
from .simpleflock import SimpleFlock
import re


def gather(database_entry, database_path):
    Gatherer(database_entry, database_path).run()


class Gatherer:
    def __init__(self, database_entry, database_path):
        self.database_entry = database_entry
        self.job_path = database_entry.output_path(database_path.parent).parent
        self.input_path = self.job_path / "input"
        self.data_path = self.job_path / "data_output"
        self.data_path.mkdir(exist_ok=True)
        self.task_files = [t for t in self.data_path.iterdir() if str(t).endswith("_task_output.json")]

        self.average_results = database_entry["average_results"]

        self.total_result = defaultdict(lambda: Dataset())
        self.total_result_of_finished = defaultdict(lambda: Dataset())
        self.successful_runs = []
        self.failed_runs = []
        self.error_message = ""
        self.error_run_id = -1
        self.raw_results_map = {}
        self.combined_task = dict(
            successful_runs=[],
            failed_runs=[],
            raw_results_map={},
            task_result=defaultdict(lambda: Dataset())
        )

    def run(self):
        finished_task_files = []

        for task_file in self.task_files:
            with SimpleFlock(str(task_file) + ".lock"):
                with open(task_file, 'r') as f:
                    task_output = json.load(f)

            if task_output["failed_runs"]:
                self.failed_runs += task_output["failed_runs"]
                self.error_message = task_output["error_message"]["message"]
                self.error_run_id = task_output["error_message"]["run_id"]

            self.raw_results_map.update(task_output["raw_results_map"] or {})

            if not task_output["successful_runs"]:
                continue

            self.successful_runs += task_output["successful_runs"]

            if self.average_results is None:
                continue

            task_result = {}
            for i, r in enumerate(task_output["task_result"]):
                if self.to_be_averaged(i):
                    task_result[i] = Dataset.from_json(r)
                    self.total_result[i] += task_result[i]
                else:
                    task_result[i] = json.loads(json.dumps(r), cls=NumpyDecoder)
                    self.total_result[i] = task_result[i]

            if task_output["done"]:
                finished_task_files.append(task_file)
                self.combined_task["successful_runs"] += task_output["successful_runs"]
                self.combined_task["failed_runs"] += task_output["failed_runs"]
                self.combined_task["error_message"] = task_output["error_message"]
                if task_output["raw_results_map"] is not None:
                    self.combined_task["raw_results_map"].update(task_output["raw_results_map"])
                else:
                    self.combined_task["raw_results_map"] = None

                for i, r in task_result.items():
                    if self.to_be_averaged(i):
                        self.combined_task["task_result"][i] += r
                    else:
                        self.combined_task["task_result"][i] = r

        self.dump()
        if len(finished_task_files) > 1:
            for f in finished_task_files:
                f.unlink()
            self.save_combined_task()

    def save_combined_task(self):
        task_result_list = [self.combined_task["task_result"][i] for i in sorted(self.combined_task["task_result"])]
        self.combined_task["task_result"] = [
            task_result.to_json() if isinstance(task_result, Dataset) else task_result
            for task_result in task_result_list
        ]

        largest_task_id = max(int(re.search(r"\d+", task_file.name).group()) for task_file in self.task_files)
        with open(self.data_path / f"{largest_task_id + 100000}_task_output.json", 'w') as f:
            json.dump(
                dict(done=True, **self.combined_task),
                f,
                indent=2,
                cls=NumpyEncoder
            )

    def to_be_averaged(self, i):
        return self.average_results is not None and (self.average_results == 'all' or i in self.average_results)

    def dump(self):
        total_result_list = [self.total_result[i] for i in sorted(self.total_result)]

        output = {
            "successful_runs": self.successful_runs,
            "failed_runs": self.failed_runs,
            "error_message": self.error_message,
            "error_run_id": self.error_run_id,
            "raw_results_map": self.raw_results_map
        }
        if self.average_results is not None:
            output.update({
                "result": polish([
                    r.mean if self.to_be_averaged(i) else r
                    for i, r in enumerate(total_result_list)
                ]),
                "estimated_error": polish([
                    r.estimated_error if self.to_be_averaged(i) else None
                    for i, r in enumerate(total_result_list)
                ]),
                "estimated_variance": polish([
                    r.estimated_variance if self.to_be_averaged(i) else None
                    for i, r in enumerate(total_result_list)
                ]),
                "total_weight": polish([
                    r.total_weight if self.to_be_averaged(i) else None
                    for i, r in enumerate(total_result_list)
                ])
            })

        with open(self.job_path / "output.json", 'w') as f:
            json.dump(output, f, indent=2, cls=NumpyEncoder)


def polish(x):
    if isinstance(x, (list, tuple)) and len(x) == 1:
        x = x[0]
    return x
