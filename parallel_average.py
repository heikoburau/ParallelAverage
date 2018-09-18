import os
import numpy as np
import json
import dill
import time
import multiprocessing as mp
from subprocess import run, PIPE
from pathlib import Path
from shutil import rmtree
from warnings import warn
from copy import deepcopy
from .simpleflock import SimpleFlock
from .json_numpy import NumpyEncoder, NumpyDecoder
try:
    import objectpath
except ImportError:
    pass


def averages_match(averageA, averageB):
    return all(averageA[key] == averageB[key] for key in [
        "function_name", "args", "kwargs", "N_runs", "average_arrays", "compute_std"
    ])


def cleaned_average(average, encoder):
    cleaned_args = json.loads(
        json.dumps(average["args"], cls=encoder),
    )
    cleaned_kwargs = json.loads(
        json.dumps(average["kwargs"], cls=encoder),
    )

    result = deepcopy(average)
    result["args"] = cleaned_args
    result["kwargs"] = cleaned_kwargs

    return result


def add_average_to_database(average, encoder):
    parallel_average_path = Path('.') / ".parallel_average"
    database_path = parallel_average_path / "database.json"

    with SimpleFlock(str(parallel_average_path / "dblock")):
        with open(database_path, 'r+') as f:
            if database_path.stat().st_size == 0:
                averages = []
            else:
                averages = json.load(f)

            averages = [a for a in averages if not averages_match(a, average)]
            averages.append(average)
            f.seek(0)
            json.dump(averages, f, indent=2, cls=encoder)
            f.truncate()


def collect_task_results(average, N_runs, failed_runs_tolerance, encoder, decoder):
    parallel_average_path = Path('.') / ".parallel_average"
    database_path = parallel_average_path / "database.json"
    job_path = parallel_average_path / average["job_name"]
    package_path = Path(os.path.abspath(__file__)).parent

    run([f"{package_path}/average_collector.sh", str(job_path.resolve()), str(package_path)])

    with open(average["output"]) as f:
        json_output = json.load(f, cls=decoder)
        output = json_output["result"]
        failed_runs = json_output["failed_runs"]

    if failed_runs:
        error_message = json_output["error_message"]
        error_run_id = json_output["error_run_id"]

        message_failed = (
            f"{len(failed_runs)} / {N_runs} runs failed!\nError message of run {error_run_id}:\n\n" +
            error_message
        )
        if len(failed_runs) > failed_runs_tolerance:
            raise RuntimeError(message_failed)
        else:
            warn(message_failed)
            average["warning message"] = message_failed

    average["status"] = "completed"
    add_average_to_database(average, encoder)

    return output


def parallel_average(
    N_runs,
    N_tasks,
    N_threads=1,
    average_arrays='all',
    compute_std=None,
    save_interpreter_state=True,
    ignore_cache=False,
    async=True,
    failed_runs_tolerance=0,
    dynamic_load_balancing=False,
    encoder=NumpyEncoder,
    decoder=NumpyDecoder
):
    def decorator(function):
        def wrapper(*args, **kwargs):
            parallel_average_path = Path('.') / ".parallel_average"
            parallel_average_path.mkdir(exist_ok=True)
            database_path = parallel_average_path / "database.json"
            database_path.touch()

            if not ignore_cache and database_path.stat().st_size > 0:
                with SimpleFlock(str(parallel_average_path / "dblock")):
                    with database_path.open() as f:
                        averages = json.load(f)

                current_average = cleaned_average(
                    {
                        "function_name": function.__name__,
                        "args": args,
                        "kwargs": kwargs,
                        "N_runs": N_runs,
                        "average_arrays": average_arrays,
                        "compute_std": compute_std
                    },
                    encoder
                )

                for average in averages:
                    if averages_match(average, current_average):
                        if average["status"] == "running":
                            print("job is still running")
                            async_result = AsyncResult(average, N_runs, failed_runs_tolerance, encoder=encoder, decoder=decoder)
                            if async:
                                return async_result
                            else:
                                return async_result.resolve()

                        if "warning message" in average:
                            warn(average["warning message"])
                        with open(average["output"]) as f_output:
                            output = json.load(f_output, cls=decoder)
                            return output["result"]

            job_name = function.__name__ + str(
                int(hash(function.__name__) + id(args) + id(kwargs)) % 100000000
            )
            print("running job-array", job_name)

            job_path = parallel_average_path / job_name
            input_path = job_path / "input"
            input_path.mkdir(parents=True, exist_ok=True)

            if dynamic_load_balancing:
                N_static_runs = N_runs // 4
                N_dynamic_runs = N_runs - N_static_runs
                chunk_size = max(1, N_dynamic_runs // 3 // N_tasks)
                dynamic_slices = list(range(N_static_runs, N_runs, chunk_size)) + [N_runs]
                chunks = list(zip(dynamic_slices[:-1], dynamic_slices[1:]))
                with (input_path / "chunks.json").open('w') as f:
                    json.dump(chunks, f)

            with (input_path / "run_task_arguments.json").open('w') as f:
                json.dump(
                    {
                        "N_runs": N_runs,
                        "N_tasks": N_tasks,
                        "N_threads": N_threads,
                        "average_arrays": average_arrays,
                        "compute_std": compute_std,
                        "save_interpreter_state": save_interpreter_state,
                        "dynamic_load_balancing": dynamic_load_balancing,
                        "N_static_runs": N_static_runs if "N_static_runs" in locals() else None
                    },
                    f
                )

            with (input_path / "run_task.d").open('wb') as f:
                dill.dump(
                    {
                        "function": function,
                        "args": args,
                        "kwargs": kwargs,
                        "encoder": encoder
                    },
                    f
                )

            if save_interpreter_state:
                dill.dump_session(str(input_path / "session.sess"))

            with (job_path / "collector_arguments.d").open('wb') as f:
                dill.dump(
                    {
                        "average_arrays": average_arrays,
                        "encoder": encoder,
                        "decoder": decoder
                    },
                    f
                )

            output_path = job_path / "output.json"
            new_average = cleaned_average(
                {
                    "function_name": function.__name__,
                    "args": args,
                    "kwargs": kwargs,
                    "N_runs": N_runs,
                    "average_arrays": average_arrays,
                    "compute_std": compute_std,
                    "output": str(output_path.resolve()),
                    "job_name": job_name,
                    "status": "running"
                },
                encoder
            )

            package_path = str(Path(os.path.abspath(__file__)).parent)
            run([
                f"{package_path}/submit_job.sh",
                str(job_path.resolve()),
                package_path,
                f"-t 1-{N_tasks} -N {job_name}",
            ])

            add_average_to_database(new_average, encoder)

            async_result = AsyncResult(new_average, N_runs, failed_runs_tolerance, encoder=encoder, decoder=decoder)

            if async:
                return async_result
            else:
                return async_result.resolve()

        return wrapper
    return decorator


class AsyncResult:
    def __init__(self, average, *params, encoder, decoder):
        self.average = average
        self.params = params
        self.encoder = encoder
        self.decoder = decoder

    def resolve(self):
        if hasattr(self, "output"):
            return self.output

        job_name = self.average["job_name"]

        while True:
            result = run(["qstat", "-r"], stdout=PIPE)
            if str(job_name) not in str(result.stdout):
                break
            time.sleep(2)

        self.output = collect_task_results(self.average, *self.params, encoder=self.encoder, decoder=self.decoder)

        return self.output


def wait_for_result(async_job):
    if hasattr(async_job, "resolve"):
        return async_job.resolve()
    return async_job


def cleanup(remove_running_jobs=False):
    parallel_average_path = Path('.') / ".parallel_average"
    database_path = parallel_average_path / "database.json"
    if not database_path.exists() or database_path.stat().st_size == 0:
        return

    with open(database_path, "r+") as f:
        database_json = json.load(f)
        if remove_running_jobs:
            database_json = [average for average in database_json if "status" not in average or average["status"] == "completed"]
            f.seek(0)
            json.dump(database_json, f, indent=2)
            f.truncate()

    database_jobs = {average["job_name"] for average in database_json}
    existing_jobs = {job.name for job in parallel_average_path.iterdir() if job.is_dir()}
    bad_jobs = existing_jobs - database_jobs

    for bad_job in bad_jobs:
        rmtree(str(parallel_average_path / bad_job))


class Database:
    def __init__(self):
        self.refresh()

    def refresh(self):
        database_path = Path('.') / ".parallel_average" / "database.json"
        if not database_path.exists() or database_path.stat().st_size == 0:
            self.db = None
            return

        with database_path.open() as f:
            self.db = json.load(f, cls=NumpyDecoder)

    @property
    def function_names(self):
        if self.db is None:
            return []

        return sorted(list({average["function_name"] for average in self.db}))

    def query(self, query_string):
        if self.db is None:
            return []

        if "objectpath" not in globals():
            raise ModuleNotFoundError("Please install the `objectpath` library in order to use this function.")

        tree = objectpath.Tree(self.db)
        return list(tree.execute(query_string))


database = Database()
