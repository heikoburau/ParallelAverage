import os
import numpy as np
import json
import dill
import multiprocessing as mp
from subprocess import run
from pathlib import Path
from shutil import rmtree
from .simpleflock import SimpleFlock
try:
    import objectpath
except ImportError:
    pass


def transform_json_output(output):
    is_numpy_array = output["is_numpy_array"]
    result = output["result"]

    if is_numpy_array is True:
        return np.array(result)
    if isinstance(is_numpy_array, (list, tuple)):
        return [
            np.array(r) if is_npa else r
            for r, is_npa in zip(result, is_numpy_array)
        ]
    return result


def averages_match(averageA, averageB):
    return all(averageA[key] == averageB[key] for key in [
        "function_name", "args", "kwargs", "N_runs", "average_arrays"
    ])


def run_average(average, N_tasks, job_path, ignore_cache, queue=None):
    package_path = str(Path(os.path.abspath(__file__)).parent)
    parallel_average_path = Path('.') / ".parallel_average"
    database_path = parallel_average_path / "database.json"

    function_name = average["function_name"]
    run([
        f"{package_path}/submit_job.sh",
        str(job_path.resolve()),
        package_path,
        f"-t 1-{N_tasks} -N {function_name}",
    ])

    with open(average["output"]) as f:
        json_output = json.load(f)
        output = transform_json_output(json_output)
        failed_tasks = json_output["failed_tasks"]

    if failed_tasks:
        failed_task = failed_tasks[0]
        raise RuntimeError(
            f"{len(failed_tasks)} / {N_tasks} tasks failed!\nError message of task {failed_task['task_id']} with run_id = {failed_task['run_id']}:\n\n" +
            failed_task["error message"]
        )

    with SimpleFlock(str(parallel_average_path / "dblock")):
        with open(database_path, 'r+') as f:
            if database_path.stat().st_size == 0:
                averages = []
            else:
                averages = json.load(f)

            if ignore_cache:
                for dublicate_average in filter(lambda a: averages_match(a, average), averages):
                    dublicate_job = parallel_average_path / dublicate_average["job_name"]
                    if dublicate_job.exists():
                        rmtree(str(dublicate_job))
                averages = [a for a in averages if not averages_match(a, average)]
            averages.append(average)
            f.seek(0)
            json.dump(averages, f, indent=2)
            f.truncate()

    if queue:
        queue.put(output)
    else:
        return output


def parallel_average(
    N_runs,
    N_tasks,
    average_arrays='all',
    save_interpreter_state=False,
    ignore_cache=False,
    async=False
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

                cleaned_args = json.loads(json.dumps(args))
                cleaned_kwargs = json.loads(json.dumps(kwargs))

                for average in averages:
                    if averages_match(
                        average,
                        {
                            "function_name": function.__name__,
                            "args": cleaned_args,
                            "kwargs": cleaned_kwargs,
                            "N_runs": N_runs,
                            "average_arrays": average_arrays
                        }
                    ):
                        with open(average["output"]) as f_output:
                            output = json.load(f_output)
                            return transform_json_output(output)

            job_name = str(
                int(hash(function.__name__) + id(args) + id(kwargs)) % 100000000
            )
            print("running job-array", job_name)

            job_path = parallel_average_path / job_name
            input_path = job_path / "input"
            input_path.mkdir(parents=True, exist_ok=True)

            with (input_path / "run_task_arguments.json").open('w') as f:
                json.dump(
                    {
                        "N_runs": N_runs,
                        "N_tasks": N_tasks,
                        "average_arrays": average_arrays,
                        "save_interpreter_state": save_interpreter_state
                    },
                    f
                )

            with (input_path / "function.d").open('wb') as f:
                dill.dump(function, f)
            with (input_path / "args.d").open('wb') as f:
                dill.dump(args, f)
            with (input_path / "kwargs.d").open('wb') as f:
                dill.dump(kwargs, f)

            if save_interpreter_state:
                dill.dump_session(str(input_path / "session.sess"))

            with (job_path / "collector_arguments.json").open('w') as f:
                json.dump(average_arrays, f)

            output_path = job_path / "output.json"
            new_average = {
                "function_name": function.__name__,
                "args": list(args),
                "kwargs": kwargs,
                "N_runs": N_runs,
                "average_arrays": average_arrays,
                "output": str(output_path.resolve()),
                "job_name": job_name
            }

            if async:
                queue = mp.Queue()
                process = mp.Process(
                    target=run_average,
                    args=(new_average, N_tasks, job_path, ignore_cache, queue)
                )
                process.start()
                return AsyncResult(process, queue)
            else:
                return run_average(new_average, N_tasks, job_path, ignore_cache)

        return wrapper
    return decorator


class AsyncResult:
    def __init__(self, process, queue):
        self.process = process
        self.queue = queue

    def resolve(self):
        if hasattr(self, "output"):
            return self.output
        self.output = self.queue.get()
        self.process.join()
        return self.output

    def __getstate__(self):
        return False

    def __setstate__(self, state):
        pass


def cleanup():
    parallel_average_path = Path('.') / ".parallel_average"
    database_path = parallel_average_path / "database.json"
    if not database_path.exists() or database_path.stat().st_size == 0:
        return

    with open(database_path) as f:
        database_json = json.load(f)

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
            self.db = json.load(f)

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
