from .database import add_average_to_database, read_database
from .AsyncResult import AsyncResult
from .average import cleaned_average
from .json_numpy import NumpyEncoder, NumpyDecoder

import os
import json
import dill
import re
from subprocess import run
from pathlib import Path
from shutil import rmtree
from datetime import datetime
from functools import wraps


package_path = Path(os.path.abspath(__file__)).parent
config_path = Path.home() / ".config/ParallelAverage"
do_submit_argname = "__parallel_average_do_submit__"
dont_submit_argname = "__parallel_average_dont_submit__"


class CachedAverageDoesNotExist(RuntimeError):
    pass


def load_cached_average(path, current_average, encoder, decoder):
    for average in read_database(path):
        if average == current_average:
            return AsyncResult(path, average, encoder=encoder, decoder=decoder).collect_task_results()

    raise CachedAverageDoesNotExist()


def find_best_fitting_averages_in_database(path, average):
    return sorted(read_database(path), key=lambda db_average: db_average.distance_to(average))[:3]


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
    job_name,
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
                "job_name": job_name,
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


def setup_and_submit_to_slurm(N_tasks, job_name, job_path, user_options):
    options = {
        "array": f"1-{N_tasks}",
        "job-name": job_name,
        "chdir": str(job_path.resolve()),
    }
    options.update(user_options)

    options_str = ""
    for name, value in options.items():
        name = name.replace('_', '-')
        if len(name) == 1:
            options_str += f"#SBATCH -{name} {value}\n"
        else:
            options_str += f"#SBATCH --{name}={value}\n"

    template_file_name = "job_script_slurm.template"
    template_file = config_path / template_file_name
    if not template_file.exists():
        template_file = package_path / template_file_name

    with (template_file).open('r') as f:
        job_script_slurm = f.read().format(slurm_options=options_str)

    with (job_path / "job_script_slurm.sh").open('w') as f:
        f.write(job_script_slurm)

    run([
        "sbatch",
        f"{job_path}/job_script_slurm.sh",
        f"{package_path}/run_task.py",
        str(Path(".").resolve())
    ])


def largest_existing_job_index(parallel_average_path):
    dirs_starting_with_a_number = [
        d.name for d in parallel_average_path.iterdir() if d.is_dir() and d.name[:1].isdigit()
    ]
    if not dirs_starting_with_a_number:
        return None

    return max(int(re.search(r"\d+", dir_str).group()) for dir_str in dirs_starting_with_a_number)


def parallel_average(
    N_runs,
    N_tasks,
    N_threads=1,
    average_results='all',
    save_interpreter_state=True,
    ignore_cache=False,  # deprecated
    force_caching=False,  # deprecated
    dynamic_load_balancing=False,
    encoder=NumpyEncoder,
    decoder=NumpyDecoder,
    path=".",
    queuing_system="Slurm",
    **queuing_system_options
):
    def decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            do_submit = do_submit_argname in kwargs
            dont_submit = dont_submit_argname in kwargs
            assert not (do_submit and dont_submit), "Both using 'do_submit' and 'dont_submit' is bullshit."
            if do_submit:
                del kwargs[do_submit_argname]
            if dont_submit:
                del kwargs[dont_submit_argname]

            assert N_runs >= 1, "'N_runs' has to be one or greater than one."

            parallel_average_path = Path(path) / ".parallel_average"
            parallel_average_path.mkdir(exist_ok=True)
            database_path = Path(path) / "parallel_average_database.json"
            database_path.touch()

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

            if not do_submit and not ignore_cache and database_path.stat().st_size > 0:
                try:
                    return load_cached_average(path, current_average, encoder, decoder)
                except CachedAverageDoesNotExist:
                    if dont_submit or force_caching:
                        best_fits_str = ""
                        for best_fit in find_best_fitting_averages_in_database(path, current_average):
                            best_fits_str += str(best_fit) + "\n\n"

                        raise CachedAverageDoesNotExist(
                            f"Best fitting averages in database:\n{best_fits_str}\n"
                            f"Invoked with:\n{current_average}"
                        )
            if dont_submit:
                return

            assert N_tasks <= N_runs, "'N_tasks' has to be less than or equal to 'N_runs'."
            assert N_tasks >= 1, "'N_tasks' has to be one or greater than one."

            job_index = (largest_existing_job_index(parallel_average_path) or 0) + 1
            job_name = f"{job_index}_{function.__name__}"
            print("running job-array", job_name)

            job_path = parallel_average_path / job_name
            input_path = job_path / "input"
            input_path.mkdir(parents=True, exist_ok=True)

            if dynamic_load_balancing:
                N_static_runs = setup_dynamic_load_balancing(N_runs, N_tasks, input_path)
            else:
                N_static_runs = None

            setup_task_input_data(
                job_name, input_path, N_runs, N_tasks, N_threads, average_results, save_interpreter_state,
                dynamic_load_balancing, N_static_runs,
                function, args, kwargs, encoder
            )
            setup_collector_input_data(input_path, average_results, encoder, decoder)

            if queuing_system == "Slurm":
                setup_and_submit_to_slurm(N_tasks, job_name, job_path, queuing_system_options)
            else:
                raise ValueError(f"Unknown queuing_system: {queuing_system}. Until now, only 'Slurm' is supported.")

            current_average["output"] = str((job_path / "output.json").resolve())
            current_average["job_name"] = job_name
            current_average["status"] = "running"
            current_average["datetime"] = datetime.now().isoformat()

            add_average_to_database(path, current_average, encoder)

            return AsyncResult(path, current_average, encoder=encoder, decoder=decoder)

        return wrapper
    return decorator


def do_submit(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[do_submit_argname] = True
        return wrapper(*args, **kwargs)

    return f


def dont_submit(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[dont_submit_argname] = True
        return wrapper(*args, **kwargs)

    return f


def cleanup(
    remove_running_jobs=False,
    remove_intermediate_files_of_completed_jobs=False,
    path="."
):
    parallel_average_path = Path(path) / ".parallel_average"
    database_path = Path(path) / "parallel_average_database.json"
    if not database_path.exists() or database_path.stat().st_size == 0:
        return

    with open(database_path, "r+") as f:
        database_json = json.load(f)
        if remove_running_jobs:
            database_json = [average for average in database_json if "status" not in average or average["status"] == "completed"]
            f.seek(0)
            json.dump(database_json, f, indent=2)
            f.truncate()

    if remove_intermediate_files_of_completed_jobs:
        completed_jobs = [average for average in database_json if average["status"] == "completed"]
        for average in completed_jobs:
            job_dir = parallel_average_path / average["job_name"]
            dirs_starting_with_a_number = [
                d for d in job_dir.iterdir() if d.is_dir() and d.name[:1].isdigit()
            ]
            for dir_ in dirs_starting_with_a_number:
                rmtree(str(dir_))
            out_files = [
                f for f in job_dir.iterdir() if f.name.endswith(".out")
            ]
            for out_file in out_files:
                out_file.unlink()

    database_jobs = {average["job_name"] for average in database_json}
    existing_jobs = {job.name for job in parallel_average_path.iterdir() if job.is_dir()}
    bad_jobs = existing_jobs - database_jobs

    for bad_job in bad_jobs:
        rmtree(str(parallel_average_path / bad_job))


def plot_average(
    x, average, label=None, color=0, points=False, linestyle="-", alpha=None, cmap="CMRmap_r",
    estimated_error=None
):
    import matplotlib.pyplot as plt

    if type(color) is int:
        color = ["tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple", "tab:brown", "tab:pink"][color]
    elif type(color) is float:
        from matplotlib.cm import ScalarMappable
        color = ScalarMappable(cmap=cmap).to_rgba(color, norm=False)

    if estimated_error is None:
        estimated_error = average.estimated_error

    if points:
        plt.errorbar(
            x,
            average,
            yerr=estimated_error,
            marker="o",
            linestyle="None",
            color=color,
            label=label,
            alpha=alpha
        )
    else:
        plt.plot(x, average, label=label, color=color, linestyle=linestyle, alpha=alpha)
        plt.fill_between(
            x,
            average - estimated_error,
            average + estimated_error,
            facecolor=color,
            alpha=0.25 * (alpha or 1)
        )


class WeightedSample:
    def __init__(self, sample, weight):
        self.sample = sample
        self.weight = weight
