from .AverageCollector import AverageCollector
from copy import deepcopy
import numpy as np
from pathlib import Path
from warnings import warn
import json


def load_averaged_result(database_entry, database_path, encoder, decoder):
    if database_entry["status"] == "running":
        job_path = Path(database_entry["output"]).parent
        AverageCollector(job_path, database_entry["average_results"], encoder, decoder).run()

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

    if output["successful_runs"]:
        num_still_running = database_entry["N_runs"] - num_finished_runs
        if num_still_running > 0:
            warn(f"{num_still_running} / {database_entry['N_runs']} runs are still running!")
        elif database_entry["status"] == "running":
            database_entry["status"] = "completed"
            database_entry.save(database_path)

    return AveragedResult(
        output["result"],
        output["estimated_error"],
        output["estimated_variance"],
        output["successful_runs"],
        output["failed_runs"],
        database_entry["job_name"]
    )


class AveragedResult:
    def __new__(cls, obj, estimated_error, *args):
        if estimated_error is None:
            return obj

        new_type_dict = dict(AveragedResult.__dict__)
        del new_type_dict["__new__"]

        new_type = type("AveragedResult", type(obj), new_type_dict)
        if isinstance(obj, np.ndarray):
            return obj.view(new_type)

        return new_type.__new__(new_type)

    def __init__(
        self,
        obj,
        estimated_error,
        estimated_variance,
        successful_runs,
        failed_runs,
        job_name,
    ):
        if not isinstance(obj, np.ndarray):
            super().__init__(obj)

        self.estimated_error = deepcopy(estimated_error)
        self.estimated_variance = deepcopy(estimated_variance)
        self.successful_runs = successful_runs
        self.failed_runs = failed_runs
        self.job_name = job_name

    @property
    def __fields(self):
        return (
            self.estimated_error,
            self.estimated_variance,
            self.successful_runs,
            self.failed_runs,
            self.job_name
        )

    def __getitem__(self, idx):
        return AveragedResult(
            super()[idx],
            self.estimated_error[idx],
            self.estimated_variance[idx],
            self.successful_runs,
            self.failed_runs,
            self.job_name
        )

    def __imul__(self, x):
        super().__imul__(x)
        self.estimated_error *= abs(x)
        self.estimated_variance *= abs(x)**2
        return self

    def __mul__(self, x):
        result = AveragedResult(deepcopy(super()), *self.__fields)
        result *= x
        return result

    def __rmul__(self, x):
        return self * x

    def __itruediv__(self, x):
        super().__itruediv__(x)
        self.estimated_error /= abs(x)
        self.estimated_variance /= abs(x)**2
        return self

    def __truediv__(self, x):
        result = AveragedResult(deepcopy(super()), *self.__fields)
        result /= x
        return result

    def __iadd__(self, x):
        super().__iadd__(self, x)
        return self

    def __add__(self, x):
        return AveragedResult(super() + x, *self.__fields)

    def __radd__(self, x):
        return self + x

    def __isub__(self, x):
        super().__isub__(self, x)
        return self

    def __sub__(self, x):
        return AveragedResult(super() - x, *self.__fields)

    def __rsub__(self, x):
        return AveragedResult(x - super(), *self.__fields)

    def __neg__(self):
        return AveragedResult(-super(), *self.__fields)

    def __pos__(self):
        return +super()

    # def __getstate__(self):
    #     return False

    # def __setstate__(self, state):
    #     pass
