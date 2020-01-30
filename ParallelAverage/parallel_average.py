from .DatabaseEntry import DatabaseEntry, load_database
from .Collector import Collector
from .AveragedResult import load_averaged_result
from .CollectiveResult import load_collective_result
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
from collections import namedtuple
import pickle


package_path = Path(os.path.abspath(__file__)).parent
action_argname = "__parallel_average_action__"

# list of actions
actions = namedtuple(
    "Actions",
    "default do_submit dont_submit print_job_output cancel_job"
)._make(range(5))


queuing_system_modules = {
    "Slurm": slurm,
    None: local_machine
}


class EntryDoesNotExist(ValueError):
    pass


def find_best_fitting_entries_in_database(database_path, entry):
    return sorted(load_database(database_path), key=lambda db_entry: db_entry.distance_to(entry))[:3]


def setup_dynamic_load_balancing(N_runs, N_tasks, input_path):
    N_runs = volume(N_runs)
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
    encoding
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
                "keep_runs": keep_runs,
                "encoding": encoding
            },
            f
        )

    with (input_path / "run_task.d").open('wb') as f:
        dill.dump(
            {
                "function": function,
                "args": args,
                "kwargs": kwargs
            },
            f
        )

    if save_interpreter_state:
        removed_objects = {}
        for name, obj in _main_module.__dict__.items():
            if name in ("In", "Out") or (
                name != "__name__" and name.startswith("_")
            ):
                removed_objects[name] = obj
                continue

            try:
                dill.dumps(obj)
            except (pickle.PicklingError, TypeError):
                removed_objects[name] = obj

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


def check_result(database_entry, database_path, path):
    with open(database_entry.output_path(path)) as f:
        output = json.load(f)

    num_finished_runs = len(output["successful_runs"]) + len(output["failed_runs"])

    if output["failed_runs"]:
        print(
            f"[ParallelAverage] Warning: {len(output['failed_runs'])} / {num_finished_runs} runs failed!\n"
            f"[ParallelAverage] Error message of run {output['error_run_id']}:\n\n"
            f"{output['error_message']}"
        )

    num_still_running = volume(database_entry["N_runs"]) - num_finished_runs
    if num_still_running > 0:
        print(f"[ParallelAverage] Warning: {num_still_running} / {volume(database_entry['N_runs'])} runs are not ready yet!")
    elif database_entry["status"] == "running":
        database_entry["status"] = "completed"
        database_entry.save(database_path)

    return len(output["successful_runs"]) > 0


def load_result(function_name, args, kwargs, N_runs, path, encoding="json"):
    database_path = Path(path) / "parallel_average_database.json"

    new_entry = DatabaseEntry({
        "function_name": function_name,
        "args": args,
        "kwargs": kwargs,
        "N_runs": N_runs,
        "average_results": None
    })

    try:
        entry = next(entry for entry in load_database(database_path) if entry == new_entry)
    except StopIteration:
        best_fits_str = ""
        for best_fit in find_best_fitting_entries_in_database(database_path, new_entry):
            best_fits_str += str(best_fit) + "\n\n"
        raise EntryDoesNotExist(
            f"Best fitting entries in database:\n{best_fits_str}\n"
            f"Invoked with:\n{new_entry}"
        )

    Collector(entry, database_path, NumpyEncoder, NumpyDecoder).run()
    if check_result(entry, database_path, path):
        if entry["average_results"] is None:
            return load_collective_result(entry, path, encoding)
        else:
            return load_averaged_result(entry, path)


def load_job_name(job_name, path, encoding="json"):
    database_path = Path(path) / "parallel_average_database.json"
    try:
        entry = next(entry for entry in load_database(database_path) if entry["job_name"] == job_name)
    except StopIteration:
        raise EntryDoesNotExist(f"'{job_name}' was not found in {path}")

    if check_result(entry, database_path, path):
        if entry["average_results"] is None:
            return load_collective_result(entry, path, encoding)
        else:
            return load_averaged_result(entry, path)


def parallel_average(
    N_runs,
    N_tasks,
    average_results='all',
    save_interpreter_state=True,
    keep_runs=False,
    dynamic_load_balancing=False,
    encoding="json",
    path=".",
    queuing_system="Slurm",
    **queuing_system_options
):
    if N_tasks == "max":
        N_tasks = volume(N_runs)

    assert encoding in ["json", "pickle"]

    def decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            action = kwargs.get(action_argname, actions.default)
            if action_argname in kwargs:
                del kwargs[action_argname]

            assert volume(N_runs) >= 1, "'N_runs' has to be one or greater than one."

            parallel_average_path = Path(path) / ".parallel_average"
            parallel_average_path.mkdir(exist_ok=True)
            database_path = Path(path) / "parallel_average_database.json"
            database_path.touch()

            if queuing_system in queuing_system_modules:
                queuing_system_module = queuing_system_modules[queuing_system]
            else:
                raise ValueError(
                    f"Unknown queuing system: {queuing_system}\n"
                    f"Supported options are: {list(queuing_system_modules)}"
                )

            new_entry = DatabaseEntry(
                {
                    "function_name": function.__name__,
                    "args": args,
                    "kwargs": kwargs,
                    "N_runs": N_runs,
                    "average_results": average_results
                }
            )

            if action != actions.do_submit and database_path.stat().st_size > 0:
                for entry in load_database(database_path):
                    if entry == new_entry:
                        if action == actions.print_job_output:
                            return queuing_system_module.print_job_output(
                                parallel_average_path / entry["job_name"]
                            )
                        elif action == actions.cancel_job:
                            queuing_system_module.cancel_job(entry["job_name"])
                            entry.remove(database_path)
                            cleanup(path=path)
                            return
                        else:
                            if entry["status"] != "completed":
                                Collector(entry, database_path).run()
                            if not check_result(entry, database_path, path):
                                return

                            if entry["average_results"] is None:
                                return load_collective_result(entry, path, encoding)
                            else:
                                return load_averaged_result(entry, path)

                if action != actions.default:
                    best_fits_str = ""
                    for best_fit in find_best_fitting_entries_in_database(database_path, new_entry):
                        best_fits_str += str(best_fit) + "\n\n"

                    raise EntryDoesNotExist(
                        f"Best fitting entries in database:\n{best_fits_str}\n"
                        f"Invoked with:\n{new_entry}"
                    )
            if action not in (actions.default, actions.do_submit):
                raise EntryDoesNotExist()

            assert N_tasks <= volume(N_runs), "'N_tasks' has to be less than or equal to 'N_runs'."
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
                function, args, kwargs, encoding
            )

            queuing_system_module.submit(
                N_tasks, job_name, job_path, queuing_system_options
            )

            new_entry["output"] = str((job_path / "output.json").relative_to(path))
            new_entry["job_name"] = job_name
            new_entry["status"] = "running"
            new_entry["datetime"] = datetime.now().isoformat()
            new_entry.save(database_path)

            if action == actions.do_submit:
                cleanup(path=path)

        return wrapper
    return decorator


def parallel(
    N_runs,
    N_tasks,
    save_interpreter_state=True,
    dynamic_load_balancing=False,
    encoding="json",
    path=".",
    queuing_system="Slurm",
    **queuing_system_options
):
    def decorator(function):
        return parallel_average(
            N_runs,
            N_tasks,
            average_results=None,
            keep_runs=True,
            save_interpreter_state=save_interpreter_state,
            dynamic_load_balancing=dynamic_load_balancing,
            encoding=encoding,
            path=path,
            queuing_system=queuing_system,
            **queuing_system_options
        )(function)

    return decorator


def do_submit(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[action_argname] = actions.do_submit
        return wrapper(*args, **kwargs)

    return f


def dont_submit(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[action_argname] = actions.dont_submit
        return wrapper(*args, **kwargs)

    return f


def print_job_output(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[action_argname] = actions.print_job_output
        return wrapper(*args, **kwargs)

    return f


def cancel_job(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[action_argname] = actions.cancel_job
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
    import numpy as np

    if type(color) is int:
        color = ["tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple", "tab:brown", "tab:pink"][color]
    elif type(color) is float:
        from matplotlib.cm import ScalarMappable
        color = ScalarMappable(cmap=cmap).to_rgba(color, norm=False)

    if estimated_error is None:
        estimated_error = average.estimated_error if hasattr(average, "estimated_error") else np.zeros_like(x)

    if points:
        plt.errorbar(
            x,
            average.data if hasattr(average, "data") else average,
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


def volume(x):
    if isinstance(x, int):
        return x

    result = 1
    for x_i in x:
        result *= x_i
    return result
