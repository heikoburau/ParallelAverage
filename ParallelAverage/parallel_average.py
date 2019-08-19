from .DatabaseEntry import DatabaseEntry, load_database
from .AveragedResult import load_averaged_result
from .json_numpy import NumpyEncoder, NumpyDecoder
from .queuing_systems import slurm, local_machine

import __main__ as _main_module

import os
import json
import dill
import re
from pathlib import Path
from shutil import rmtree
from datetime import datetime
from functools import wraps


package_path = Path(os.path.abspath(__file__)).parent
action_argname = "__parallel_average_action__"

# list of actions
default_action = 0
do_submit = 1
dont_submit = 2


queuing_system_modules = {
    "Slurm": slurm,
    None: local_machine
}


class EntryDoesNotExist(RuntimeError):
    pass


def find_best_fitting_entries_in_database(database_path, entry):
    return sorted(load_database(database_path), key=lambda db_entry: db_entry.distance_to(entry))[:3]


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
    average_results,
    save_interpreter_state,
    dynamic_load_balancing,
    N_static_runs,
    keep_runs,
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
                "average_results": average_results,
                "save_interpreter_state": save_interpreter_state,
                "dynamic_load_balancing": dynamic_load_balancing,
                "N_static_runs": N_static_runs,
                "keep_runs": keep_runs
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
        removed_objects = {
            name: obj for name, obj in _main_module.__dict__.items() if (
                "AveragedResult" in str(type(obj)) or
                (hasattr(obj, "__name__") and "AveragedResult" in obj.__name__) or
                (hasattr(obj, "__name__") and obj.__name__ == "matplotlib.pyplot") or
                name in ("In", "Out") or
                (name != "__name__" and name.startswith("_"))
            )
        }

        for name in removed_objects:
            del _main_module.__dict__[name]

        dill.dump_session(str(input_path / "session.pkl"), main=_main_module)

        for name, obj in removed_objects.items():
            _main_module.__dict__[name] = obj


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
    average_results='all',
    save_interpreter_state=True,
    keep_runs=False,
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
            action = kwargs.get(action_argname, default_action)
            if action_argname in kwargs:
                del kwargs[action_argname]

            assert N_runs >= 1, "'N_runs' has to be one or greater than one."

            parallel_average_path = Path(path) / ".parallel_average"
            parallel_average_path.mkdir(exist_ok=True)
            database_path = Path(path) / "parallel_average_database.json"
            database_path.touch()

            new_entry = DatabaseEntry(
                {
                    "function_name": function.__name__,
                    "args": args,
                    "kwargs": kwargs,
                    "N_runs": N_runs,
                    "average_results": average_results
                },
                encoder
            )

            if not action == do_submit and not ignore_cache and database_path.stat().st_size > 0:
                for entry in load_database(database_path):
                    if entry == new_entry:
                        return load_averaged_result(entry, database_path, encoder, decoder)

                if action == dont_submit or force_caching:
                    best_fits_str = ""
                    for best_fit in find_best_fitting_entries_in_database(database_path, new_entry):
                        best_fits_str += str(best_fit) + "\n\n"

                    raise EntryDoesNotExist(
                        f"Best fitting entries in database:\n{best_fits_str}\n"
                        f"Invoked with:\n{new_entry}"
                    )
            if action == dont_submit:
                raise EntryDoesNotExist()

            assert N_tasks <= N_runs, "'N_tasks' has to be less than or equal to 'N_runs'."
            assert N_tasks >= 1, "'N_tasks' has to be one or greater than one."

            job_index = (largest_existing_job_index(parallel_average_path) or 0) + 1
            job_name = f"{job_index}_{function.__name__}"
            job_path = parallel_average_path / job_name
            input_path = job_path / "input"
            input_path.mkdir(parents=True, exist_ok=True)

            if dynamic_load_balancing:
                N_static_runs = setup_dynamic_load_balancing(N_runs, N_tasks, input_path)
            else:
                N_static_runs = None

            setup_task_input_data(
                job_name, input_path, N_runs, N_tasks, average_results, save_interpreter_state,
                dynamic_load_balancing, N_static_runs, keep_runs,
                function, args, kwargs, encoder
            )

            if queuing_system in queuing_system_modules:
                queuing_system_modules[queuing_system].submit(
                    N_tasks, job_name, job_path, queuing_system_options
                )
            else:
                raise ValueError(
                    f"Unknown queuing system: {queuing_system}\n"
                    f"Supported options are: {list(queuing_system_modules)}"
                )

            if queuing_system is not None:
                print("submitting job-array", job_name)
            else:
                print(f"starting {N_tasks} local processes", job_name)

            new_entry["output"] = str((job_path / "output.json").resolve())
            new_entry["job_name"] = job_name
            new_entry["status"] = "running"
            new_entry["datetime"] = datetime.now().isoformat()
            new_entry.save(database_path)

        return wrapper
    return decorator


def do_submit(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[action_argname] = do_submit
        return wrapper(*args, **kwargs)

    return f


def dont_submit(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[action_argname] = dont_submit
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
            database_json = [
                average for average in database_json if (
                    "status" not in average or average["status"] == "completed"
                )
            ]
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
            +average,
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
            +average - estimated_error,
            +average + estimated_error,
            facecolor=color,
            alpha=0.25 * (alpha or 1)
        )
