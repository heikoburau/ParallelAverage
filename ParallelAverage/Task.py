from .Dataset import Dataset
from .simpleflock import SimpleFlock
from .json_numpy import NumpyEncoder, NumpyDecoder
from collections import defaultdict
import json


class Task:
    def __init__(self, database_entry):
        self.done = False
        self.successful_runs = []
        self.failed_runs = []
        self.error_message = {}
        self.raw_results_map = {}
        self.task_result = defaultdict(lambda: Dataset())
        self.database_entry = database_entry
        self.average_results = database_entry["average_results"]

    @property
    def metainfo(self):
        return dict(
            done=self.done,
            successful_runs=self.successful_runs,
            failed_runs=self.failed_runs,
            error_message=self.error_message,
            raw_results_map=self.raw_results_map,
        )

    @property
    def as_dict(self):
        task_result_list = [self.task_result[i] for i in sorted(self.task_result)]
        task_result_json = [
            task_result.to_json() if isinstance(task_result, Dataset) else task_result
            for task_result in task_result_list
        ]

        return dict(
            **self.metainfo,
            task_result=task_result_json
        )

    @property
    def raw_results_files(self):
        return [
            self.database_entry.job_path.data_path / f"{task_id}_raw_results.json"
            for task_id in self.raw_results_map.values()
        ]

    def to_be_averaged(self, i):
        return self.average_results is not None and (self.average_results == 'all' or i in self.average_results)

    def load(self, task_output_path):
        with SimpleFlock(str(task_output_path) + ".lock"):
            with open(task_output_path, 'r') as f:
                output = json.load(f)

        self.done = output["done"]
        self.successful_runs = output["successful_runs"]
        self.failed_runs = output["failed_runs"]
        self.error_message = output["error_message"]
        self.raw_results_map = output["raw_results_map"]
        self.task_result = defaultdict(lambda: Dataset())
        for i, r in enumerate(output["task_result"]):
            if self.to_be_averaged(i):
                self.task_result[i] = Dataset.from_json(r)
            else:
                self.task_result[i] = json.loads(json.dumps(r), cls=NumpyDecoder)
        return self

    def save(self, task_output_path):
        with open(task_output_path, 'w') as f:
            json.dump(
                self.as_dict,
                f,
                indent=2,
                cls=NumpyEncoder
            )

    def incorporate(self, other):
        if not other.done:
            self.done = False
        self.successful_runs += other.successful_runs
        self.failed_runs += other.failed_runs
        if other.failed_runs:
            self.error_message = other.error_message
        if other.raw_results_map is not None:
            self.raw_results_map.update(other.raw_results_map)
        if other.successful_runs:
            for i, r in other.task_result.items():
                if self.to_be_averaged(i):
                    self.task_result[i] += r
                else:
                    self.task_result[i] = r
