from .AverageCollector import AverageCollector
from copy import deepcopy
import numpy as np
from pathlib import Path
from warnings import warn
import json


def load_averaged_result(database_entry, database_path, encoder, decoder):
    if database_entry["status"] == "running":
        job_path = Path(database_entry["output"]).parent

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

    if output["successful_runs"]:
        num_still_running = database_entry["N_runs"] - num_finished_runs
        if num_still_running > 0:
            warn(f"{num_still_running} / {database_entry['N_runs']} runs are still running!")
        elif database_entry["status"] == "running":
            database_entry["status"] = "completed"
            database_entry.save(database_path)

    return AveragedResultPrototype(
        output["result"],
        output["estimated_error"],
        output["estimated_variance"],
        output["successful_runs"],
        output["failed_runs"],
        database_entry["job_name"]
    )


class AveragedResultPrototype:
    def __new__(cls, obj, estimated_error, *args):
        if estimated_error is None:
            return obj

        new_type_dict = dict(AveragedResultPrototype.__dict__)
        del new_type_dict["__new__"]

        new_type_name = f"AveragedResult({type(obj).__name__})"
        if new_type_name not in globals():
            new_type = type(new_type_name, (type(obj),), new_type_dict)
            globals()[new_type.__name__] = new_type
        else:
            new_type = globals()[new_type_name]

        if isinstance(obj, np.ndarray):
            result = obj.view(new_type)
        else:
            result = new_type.__new__(new_type, obj)

        result.__init__(obj, estimated_error, *args)
        return result

    def __init__(
        self,
        obj,
        estimated_error,
        estimated_variance,
        successful_runs,
        failed_runs,
        job_name,
    ):
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

    def __str__(self):
        return super(self.__class__, self).__str__() + " +/- " + str(self.estimated_error)

    def __repr__(self):
        return super(self.__class__, self).__repr__() + " +/- " + repr(self.estimated_error)

    def __getitem__(self, idx):
        self_class = self.__class__
        while str(self_class.__base__) == "AveragedResult":
            self_class = self_class.__base__

        if isinstance(self, np.ndarray):
            result = self.view(self_class.__base__).__getitem__(idx)
        else:
            result = super(self_class, self).__getitem__(idx)

        if not hasattr(self, "estimated_error"):
            return result

        return AveragedResultPrototype(
            result,
            self.estimated_error[idx],
            self.estimated_variance[idx],
            self.successful_runs,
            self.failed_runs,
            self.job_name
        )

    def __imul__(self, x):
        super(self.__class__, self).__imul__(x)
        self.estimated_error *= abs(x)
        self.estimated_variance *= abs(x)**2
        return self

    def __mul__(self, x):
        result = AveragedResultPrototype(deepcopy(super(self.__class__, self)), *self.__fields)
        result *= x
        return result

    def __rmul__(self, x):
        return self * x

    def __itruediv__(self, x):
        super(self.__class__, self).__itruediv__(x)
        self.estimated_error /= abs(x)
        self.estimated_variance /= abs(x)**2
        return self

    def __truediv__(self, x):
        result = AveragedResultPrototype(deepcopy(super(self.__class__, self)), *self.__fields)
        result /= x
        return result

    def __iadd__(self, x):
        super(self.__class__, self).__iadd__(self, x)
        return self

    def __add__(self, x):
        return AveragedResultPrototype(super(self.__class__, self) + x, *self.__fields)

    def __radd__(self, x):
        return self + x

    def __isub__(self, x):
        super(self.__class__, self).__isub__(self, x)
        return self

    def __sub__(self, x):
        return AveragedResultPrototype(super(self.__class__, self) - x, *self.__fields)

    def __rsub__(self, x):
        return AveragedResultPrototype(x - super(self.__class__, self), *self.__fields)

    def __neg__(self):
        return AveragedResultPrototype(-super(self.__class__, self), *self.__fields)

    def __pos__(self):
        if isinstance(self, np.ndarray):
            return +self.view(np.ndarray)

        return super(self.__class__, self).__pos__(self)

    @property
    def real(self):
        if isinstance(self, np.ndarray):
            return AveragedResultPrototype(
                super(self.__class__, self).real.view(np.ndarray),
                *self.__fields
            )

        return AveragedResultPrototype(
            super(self.__class__, self).real,
            *self.__fields
        )

    @property
    def imag(self):
        if isinstance(self, np.ndarray):
            return AveragedResultPrototype(
                super(self.__class__, self).imag.view(np.ndarray),
                *self.__fields
            )

        return AveragedResultPrototype(
            super(self.__class__, self).imag,
            *self.__fields
        )

    def __getstate__(self):
        return False

    def __setstate__(self, state):
        pass
