"""
This class produces a total result from each individual task result it finds in the 'data_output' folder.
The total result has the same structure as an individual result.
"""

from .json_numpy import NumpyEncoder
from .Task import Task
import json


def gather(database_entry):
    Gatherer(database_entry).run().update_folder()


class Gatherer:
    def __init__(self, database_entry):
        self.database_entry = database_entry
        self.job_path = database_entry.job_path
        self.total_task = Task(self.database_entry)
        self.partial_task = Task(self.database_entry)
        self.partial_task.done = True
        self.average_results = database_entry["average_results"]

    def run(self):
        self.finished_task_files = []

        for task_file in self.job_path.task_output_files:
            task = Task(self.database_entry).load(task_file)

            self.total_task.incorporate(task)

            if task.done:
                self.finished_task_files.append(task_file)
                self.partial_task.incorporate(task)

        return self

    def update_folder(self):
        self.dump()
        if len(self.finished_task_files) > 1:
            new_task_id = max(self.job_path.task_ids or [0]) + 100000
            self.partial_task.save(self.job_path.data_path / f"{new_task_id}_task_output.json")
            for f in self.finished_task_files:
                f.unlink()

    def to_be_averaged(self, i):
        return self.average_results is not None and (self.average_results == 'all' or i in self.average_results)

    def dump(self):
        total_result_list = [self.total_task.task_result[i] for i in sorted(self.total_task.task_result)]

        output = self.total_task.metainfo
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
