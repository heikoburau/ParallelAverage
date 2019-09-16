from .AverageCollector import AverageCollector
from .json_numpy import NumpyDecoder
from copy import deepcopy
from pathlib import Path
from warnings import warn
import json


def load_averaged_result(database_entry, database_path, encoder, decoder):
    job_path = Path(database_entry["output"]).parent
    if database_entry["status"] == "running":
        try:
            AverageCollector(job_path, database_entry["average_results"], encoder, decoder).run()
        except FileNotFoundError:
            # for backward compatibility
            import os
            from subprocess import run

            package_path = Path(os.path.abspath(__file__)).parent
            average_collector_file = package_path / "legacy" / "average_collector.sh"
            run([str(average_collector_file), str(job_path.resolve()), str(package_path)])

    with open(database_entry["output"]) as f:
        output = json.load(f, cls=decoder)

    # for backward compatibility
    if "successful_runs" not in output:
        output["successful_runs"] = [0] * (output["N_total_runs"] - len(output["failed_runs"]))

    num_finished_runs = len(output["successful_runs"]) + len(output["failed_runs"])

    if output["failed_runs"]:
        warn(
            f"{len(output['failed_runs'])} / {num_finished_runs} runs failed!\n"
            f"Error message of run {output['error_run_id']}:\n\n"
            f"{output['error_message']}"
        )
        print(
            f"{len(output['failed_runs'])} / {num_finished_runs} runs failed!\n"
            f"Error message of run {output['error_run_id']}:\n\n"
            f"{output['error_message']}"
        )

    num_still_running = volume(database_entry["N_runs"]) - num_finished_runs
    if num_still_running > 0:
        warn(f"{num_still_running} / {volume(database_entry['N_runs'])} runs are not ready yet!")
    elif database_entry["status"] == "running":
        database_entry["status"] = "completed"
        database_entry.save(database_path)

    return AveragedResult(
        output["result"],
        output["estimated_error"],
        output["estimated_variance"],
        output["successful_runs"],
        output["failed_runs"],
        Runs(
            output["successful_runs"],
            job_path,
            output["raw_results_map"]
        ) if "raw_results_map" in output else None,
        database_entry["job_name"]
    )


class AveragedResult:
    def __init__(
        self,
        data,
        estimated_error,
        estimated_variance,
        successful_run_ids,
        failed_run_ids,
        runs,
        job_name,
    ):
        self.data = data
        self.estimated_error = deepcopy(estimated_error)
        self.estimated_variance = deepcopy(estimated_variance)
        self.successful_run_ids = successful_run_ids
        self.failed_run_ids = failed_run_ids
        self.runs = runs
        self.job_name = job_name

    @property
    def _meta_info_fields(self):
        return (
            self.successful_run_ids,
            self.failed_run_ids,
            self.runs,
            self.job_name
        )

    def __str__(self):
        return str(self.data) + " +/- " + str(self.estimated_error)

    def __repr__(self):
        return repr(self.data) + " +/- " + repr(self.estimated_error)

    def __getstate__(self):
        return False

    def __setstate__(self, state):
        pass

    def __getattr__(self, name):
        return getattr(self.data, name)

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, name):
        return AveragedResult(
            self.data[name],
            self.estimated_error[name],
            self.estimated_variance[name],
            *self._meta_info_fields
        )

    def __setitem__(self, name, value):
        self.data[name] = value
        return self


numeric_magic_functions = [
    "__add__",
    "__sub__",
    "__mul__",
    "__matmul__",
    "__truediv__",
    "__floordiv__",
    "__mod__",
    "__divmod__",
    "__pow__",
    "__lshift__",
    "__rshift__",
    "__and__",
    "__xor__",
    "__or__",
    "__radd__",
    "__rsub__",
    "__rmul__",
    "__rmatmul__",
    "__rtruediv__",
    "__rfloordiv__",
    "__rmod__",
    "__rdivmod__",
    "__rpow__",
    "__rlshift__",
    "__rrshift__",
    "__rand__",
    "__rxor__",
    "__ror__",
    "__iadd__",
    "__isub__",
    "__imul__",
    "__imatmul__",
    "__itruediv__",
    "__ifloordiv__",
    "__ipow__",
    "__irshift__",
    "__ixor__",
    "__ior__",
    "__neg__",
    "__pos__",
    "__abs__",
    "__invert__",
    "__round__"
]

for function in numeric_magic_functions:
    wrapper = lambda function: lambda *args: getattr(args[0].data, function)(*args[1:])
    setattr(AveragedResult, function, wrapper(function))


class Runs:
    def __init__(self, run_ids, job_path, raw_results_map):
        self.run_ids = run_ids
        self.job_path = job_path
        self.raw_results_map = raw_results_map

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


def volume(x):
    if isinstance(x, int):
        return x

    result = 1
    for x_i in x:
        result *= x_i
    return result
