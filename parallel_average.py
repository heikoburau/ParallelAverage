from .database import add_average_to_database
from .AsyncResult import AsyncResult
from .average_utils import cleaned_average, averages_match
from .simpleflock import SimpleFlock
from .json_numpy import NumpyEncoder, NumpyDecoder

import os
import json
import dill
from subprocess import run
from pathlib import Path
from shutil import rmtree


class CachedAverageDoesNotExist(RuntimeError):
    pass


def load_cached_average(function, args, kwargs, N_runs, average_results, encoder, decoder):
    parallel_average_path = Path('.') / ".parallel_average"
    database_path = parallel_average_path / "database.json"

    current_average = cleaned_average(
        {
            "function_name": function.__name__,
            "args": args,
            "kwargs": kwargs,
            "N_runs": N_runs,
            "average_results": average_results
        },
        encoder
    )

    with SimpleFlock(str(parallel_average_path / "dblock")):
        with database_path.open() as f:
            averages = json.load(f)

    for average in averages:
        if averages_match(average, current_average):
            return AsyncResult(average, N_runs, encoder=encoder, decoder=decoder).collect_task_results()

    raise CachedAverageDoesNotExist()


def setup_dynamic_load_balancing(N_runs, N_tasks, input_path):
    N_static_runs = N_runs // 4
    N_dynamic_runs = N_runs - N_static_runs
    chunk_size = max(1, N_dynamic_runs // 3 // N_tasks)
    dynamic_slices = list(range(N_static_runs, N_runs, chunk_size)) + [N_runs]
    chunks = list(zip(dynamic_slices[:-1], dynamic_slices[1:]))
    with (input_path / "chunks.json").open('w') as f:
        json.dump(chunks, f)

    return N_static_runs


def setup_task_input_data(
    input_path,
    N_runs,
    N_tasks,
    N_threads,
    average_results,
    save_interpreter_state,
    dynamic_load_balancing,
    N_static_runs,
    function,
    args,
    kwargs,
    encoder
):
    with (input_path / "run_task_arguments.json").open('w') as f:
        json.dump(
            {
                "N_runs": N_runs,
                "N_tasks": N_tasks,
                "N_threads": N_threads,
                "average_results": average_results,
                "save_interpreter_state": save_interpreter_state,
                "dynamic_load_balancing": dynamic_load_balancing,
                "N_static_runs": N_static_runs
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


def setup_collector_input_data(input_path, average_results, encoder, decoder):
    with (input_path / "collector_arguments.d").open('wb') as f:
        dill.dump(
            {
                "average_results": average_results,
                "encoder": encoder,
                "decoder": decoder
            },
            f
        )


def setup_and_submit_to_slurm(N_tasks, job_name, job_path, queuing_system_options):
    package_path = str(Path(os.path.abspath(__file__)).parent)

    options = {
        "array": f"1-{N_tasks}",
        "job-name": job_name,
        "chdir": str(job_path.resolve()),
        "time": "24:00:00"
    }
    options.update(queuing_system_options)

    options_str = ""
    for name, value in options.items():
        if len(name) == 1:
            options_str += f"#SBATCH -{name} {value}\n"
        else:
            options_str += f"#SBATCH --{name}={value}\n"

    job_script_slurm = (
        "#!/bin/bash -i\n\n"
        f"{options_str.strip()}\n\n"
        "WORK_DIR=./$SLURM_ARRAY_TASK_ID\n"
        "mkdir -p $WORK_DIR\n"
        "module purge\n"
        "module load intelpython3\n"
        "cd $WORK_DIR\n"
        "python $1 $SLURM_ARRAY_TASK_ID\n"
    )

    with (job_path / "job_script_slurm.sh").open('w') as f:
        f.write(job_script_slurm)

    run([
        "sbatch",
        f"{job_path}/job_script_slurm.sh",
        f"{package_path}/run_task.py"
    ])


def submit_to_sge(N_tasks, job_name, job_path):
    package_path = str(Path(os.path.abspath(__file__)).parent)

    run([
        f"{package_path}/submit_job.sh",
        str(job_path.resolve()),
        package_path,
        f"-t 1-{N_tasks} -N {job_name}",
    ])


def parallel_average(
    N_runs,
    N_tasks,
    N_threads=1,
    average_results='all',
    save_interpreter_state=True,
    ignore_cache=False,
    dynamic_load_balancing=False,
    encoder=NumpyEncoder,
    decoder=NumpyDecoder,
    slurm=False,
    **queuing_system_options
):
    def decorator(function):
        def wrapper(*args, **kwargs):
            parallel_average_path = Path('.') / ".parallel_average"
            parallel_average_path.mkdir(exist_ok=True)
            database_path = parallel_average_path / "database.json"
            database_path.touch()

            if not ignore_cache and database_path.stat().st_size > 0:
                try:
                    return load_cached_average(function, args, kwargs, N_runs, average_results, encoder, decoder)
                except CachedAverageDoesNotExist:
                    pass

            job_name = function.__name__ + str(
                int(hash(function.__name__) + id(args) + id(kwargs)) % 100000000
            )
            print("running job-array", job_name)

            job_path = parallel_average_path / job_name
            input_path = job_path / "input"
            input_path.mkdir(parents=True, exist_ok=True)

            if dynamic_load_balancing:
                N_static_runs = setup_dynamic_load_balancing(N_runs, N_tasks, input_path)
            else:
                N_static_runs = None

            setup_task_input_data(
                input_path, N_runs, N_tasks, N_threads, average_results, save_interpreter_state,
                dynamic_load_balancing, N_static_runs,
                function, args, kwargs, encoder
            )
            setup_collector_input_data(input_path, average_results, encoder, decoder)

            if slurm:
                setup_and_submit_to_slurm(N_tasks, job_name, job_path, queuing_system_options)
            else:
                submit_to_sge(N_tasks, job_name, job_path)

            new_average = cleaned_average(
                {
                    "function_name": function.__name__,
                    "args": args,
                    "kwargs": kwargs,
                    "N_runs": N_runs,
                    "average_results": average_results,
                    "output": str((job_path / "output.json").resolve()),
                    "job_name": job_name,
                    "status": "running"
                },
                encoder
            )
            add_average_to_database(new_average, encoder)

            return AsyncResult(new_average, N_runs, encoder=encoder, decoder=decoder)

        return wrapper
    return decorator


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


def plot_average(x, average, label=None, color=0, points=False, linestyle="-"):
    import matplotlib.pyplot as plt

    color = ["tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple", "tab:brown", "tab:pink"][color]

    if points:
        plt.errorbar(
            x,
            average,
            yerr=average.estimated_error,
            marker="o",
            linestyle="None",
            color=color,
            label=label
        )
    else:
        plt.plot(x, average, label=label, color=color, linestyle=linestyle)
        plt.fill_between(
            x,
            average - average.estimated_error,
            average + average.estimated_error,
            facecolor=color,
            alpha=0.25
        )


class WeightedSample:
    def __init__(self, sample, weight):
        self.sample = sample
        self.weight = weight
