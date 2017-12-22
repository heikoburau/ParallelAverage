import os
import numpy as np
import json
import dill
from subprocess import run
from pathlib import Path
from shutil import rmtree


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
        "function_name", "args", "kwargs", "N_total_runs", "average_arrays"
    ])


def parallel_average(N_runs, N_local_runs=1, average_arrays='all', save_interpreter_state=False, ignore_cache=False):
    def decorator(function):
        def wrapper(*args, **kwargs):
            parallel_average_path = Path('.') / ".parallel_average"
            parallel_average_path.mkdir(exist_ok=True)
            database_path = parallel_average_path / "database.json"
            database_path.touch()

            if not ignore_cache and database_path.stat().st_size > 0:
                with database_path.open() as f:
                    for average in json.load(f):
                        if averages_match(
                            average,
                            {
                                "function_name": function.__name__,
                                "args": list(args),
                                "kwargs": kwargs,
                                "N_total_runs": N_runs * N_local_runs,
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
                        "N_local_runs": N_local_runs,
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

            package_path = str(Path(os.path.abspath(__file__)).parent)

            with (job_path / "collector_arguments.json").open('w') as f:
                json.dump(average_arrays, f)

            run([
                f"{package_path}/submit_job.sh",
                str(job_path.resolve()), 
                package_path,
                f"-t 1-{N_runs}",
            ])
            output_path = job_path / "output.json"

            new_average = {
                "function_name": function.__name__,
                "args": list(args),
                "kwargs": kwargs,
                "N_total_runs": N_runs * N_local_runs,
                "average_arrays": average_arrays,
                "output": str(output_path.resolve()),
                "job_name": job_name
            }

            with open(database_path, 'r+') as f:
                if database_path.stat().st_size == 0:
                    averages = []
                else:
                    averages = json.load(f)

                if ignore_cache:
                    for dublicate_average in filter(lambda a: averages_match(a, new_average), averages):
                        rmtree(str(parallel_average_path / dublicate_average["job_name"]))
                    averages = [a for a in averages if not averages_match(a, new_average)]
                averages.append(new_average)
                f.seek(0)
                json.dump(averages, f, indent=2)
                f.truncate()

            with output_path.open() as f:
                return transform_json_output(json.load(f))

        return wrapper
    return decorator
