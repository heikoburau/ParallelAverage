from .CollectiveResult import CollectiveResult
from .json_numpy import NumpyEncoder, NumpyDecoder
from copy import deepcopy
import json


def load_averaged_result(database_entry, encoding):
    output = database_entry.output

    return AveragedResult(
        output["result"],
        output["estimated_error"],
        output["estimated_variance"],
        output["successful_runs"],
        output["failed_runs"],
        CollectiveResult(
            output["successful_runs"],
            database_entry.job_path,
            output["raw_results_map"],
            database_entry["job_name"],
            encoding
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

    def to_json(self):
        return dict(
            data=json.loads(json.dumps(self.data, cls=NumpyEncoder)),
            estimated_error=json.loads(json.dumps(self.estimated_error, cls=NumpyEncoder)),
            estimated_variance=json.loads(json.dumps(self.estimated_variance, cls=NumpyEncoder)),
            successful_run_ids=self.successful_run_ids,
            failed_run_ids=self.failed_run_ids,
            runs=None,
            job_name=self.job_name
        )

    @staticmethod
    def from_json(obj):
        return AveragedResult(
            json.loads(json.dumps(obj["data"]), cls=NumpyDecoder),
            json.loads(json.dumps(obj["estimated_error"]), cls=NumpyDecoder),
            json.loads(json.dumps(obj["estimated_variance"]), cls=NumpyDecoder),
            obj["successful_run_ids"],
            obj["failed_run_ids"],
            None,
            obj["job_name"]
        )

    def __str__(self):
        return str(self.data) + " +/- " + str(self.estimated_error)

    def __repr__(self):
        return repr(self.data) + " +/- " + repr(self.estimated_error)

    def __getstate__(self):
        return self.to_json()

    def __setstate__(self, state):
        self.__dict__.update(AveragedResult.from_json(state).__dict__)

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


def volume(x):
    if isinstance(x, int):
        return x

    result = 1
    for x_i in x:
        result *= x_i
    return result
