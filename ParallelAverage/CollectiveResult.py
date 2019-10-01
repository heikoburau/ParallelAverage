from .json_numpy import NumpyDecoder
from copy import deepcopy
from pathlib import Path
import json


def load_collective_result(database_entry):
    job_path = Path(database_entry["output"]).parent

    with open(database_entry["output"]) as f:
        output = json.load(f)

    return CollectiveResult(
        output["successful_runs"],
        job_path,
        output["raw_results_map"],
        database_entry["job_name"]
    )


class CollectiveResult:
    def __init__(self, run_ids, job_path, raw_results_map, job_name):
        self.run_ids = run_ids
        if isinstance(self.run_ids[0], str):
            self.run_ids = [eval(run_id) for run_id in self.run_ids]
        self.run_ids = sorted(self.run_ids)

        self.job_path = job_path
        self.raw_results_map = raw_results_map
        self.job_name = job_name

    def __iter__(self):
        return iter(self.run_ids)

    def __getitem__(self, run_id):
        if not isinstance(run_id, str):
            run_id = repr(run_id)

        file_id = self.raw_results_map[run_id]
        with open(self.job_path / "data_output" / f"{file_id}_raw_results.json") as f:
            return json.load(f, cls=NumpyDecoder)[run_id]

    def __len__(self):
        return len(self.run_ids)

    def keys(self):
        return self.run_ids

    def values(self):
        for run_id in self:
            yield self[run_id]

    def items(self):
        for run_id in self:
            yield run_id, self[run_id]
