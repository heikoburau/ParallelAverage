from .database import add_average_to_database
from subprocess import run
from warnings import warn
from pathlib import Path
import os
import json


package_path = Path(os.path.abspath(__file__)).parent
config_path = Path.home() / ".config/ParallelAverage"


class AsyncResult:
    def __init__(self, path, average, encoder, decoder):
        self.path = Path(path)
        self.average = average
        self.N_runs = average["N_runs"]
        self.encoder = encoder
        self.decoder = decoder
        self.has_collected = False
        self.is_complete = False
        self._result = None

    def collect_task_results(self):
        if self.is_complete:
            return self

        parallel_average_path = self.path / ".parallel_average"
        job_path = parallel_average_path / self.average["job_name"]

        if self.average["status"] == "running":
            average_collector_file = config_path / "average_collector.sh"
            if not average_collector_file.exists():
                average_collector_file = package_path / "average_collector.sh"

            run([str(average_collector_file), str(job_path.resolve()), str(package_path)])

        with open(self.average["output"]) as f:
            json_output = json.load(f, cls=self.decoder)
            self._result = json_output["result"]
            self._estimated_error = json_output.get("estimated_error")
            self._estimated_variance = json_output.get("estimated_variance")
            self._total_weights = json_output.get("total_weights")
            self._failed_runs = json_output["failed_runs"]
            self._num_completed_runs = json_output.get("N_total_runs")

        if self._failed_runs:
            error_message = json_output["error_message"]
            error_run_id = json_output["error_run_id"]

            message_failed = (
                f"{len(self._failed_runs)} / {self._num_completed_runs + len(self._failed_runs)} runs failed!\nError message of run {error_run_id}:\n\n" +
                error_message
            )
            warn(message_failed)

        if self._num_completed_runs is not None:
            N_running_runs = self.N_runs - (self._num_completed_runs + len(self._failed_runs))
            if N_running_runs == 0:
                self.is_complete = True
            if N_running_runs > 0:
                warn(f"{N_running_runs} / {self.N_runs} runs are still running!")
            elif self.average["status"] == "running":
                self.average["status"] = "completed"
                add_average_to_database(self.path, self.average, self.encoder)

        self.has_collected = True

        return self

    @property
    def result(self):
        if not self.has_collected:
            self.collect_task_results()
        return self._result

    @property
    def estimated_error(self):
        if not self.has_collected:
            self.collect_task_results()
        return self._estimated_error

    @property
    def estimated_variance(self):
        if not self.has_collected:
            self.collect_task_results()
        return self._estimated_variance

    @property
    def total_weight(self):
        if not self.has_collected:
            self.collect_task_results()
        return self._total_weights

    @property
    def failed_runs(self):
        self.collect_task_results()
        return self._failed_runs

    @property
    def num_completed_runs(self):
        self.collect_task_results()
        return self._num_completed_runs

    def __getstate__(self):
        return False

    def __setstate__(self, state):
        pass

    def __getattr__(self, name):
        return getattr(self.result, name)

    def __str__(self):
        return str(self.result)

    def __repr__(self):
        return repr(self.result)

    def __len__(self):
        return len(self.result)

    def __iter__(self):
        return iter(self.result)

    def __getitem__(self, name):
        return self.result[name]

    def __setitem__(self, name, value):
        self.result[name] = value
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
    "__invert__"
]

for function in numeric_magic_functions:
    wrapper = lambda function: lambda *args: getattr(args[0].result, function)(*args[1:])
    setattr(AsyncResult, function, wrapper(function))
