from .prepare_submission import setup_task_input_data, setup_dynamic_load_balancing
from .DatabaseEntry import DatabaseEntry, load_database
from .AveragedResult import load_averaged_result
from .CollectiveResult import load_collective_result
from .JobPath import JobPath
from .re_submit import prepare_re_submission
from .queuing_systems import slurm, local_machine

import os
import json
import re
from pathlib import Path
from shutil import rmtree
from datetime import datetime
from functools import wraps
from collections import namedtuple


package_path = Path(os.path.abspath(__file__)).parent
action_argname = "__parallel_average_action__"

# list of actions
actions = namedtuple(
    "Actions",
    "default do_submit dont_submit re_submit print_job_output cancel_job"
)._make(range(6))


queuing_system_modules = {
    "Slurm": slurm,
    None: local_machine
}


class EntryDoesNotExist(ValueError):
    pass


def largest_existing_job_index(parallel_average_path):
    dirs_starting_with_a_number = [
        d.name for d in parallel_average_path.iterdir() if d.is_dir() and d.name[:1].isdigit()
    ]
    if not dirs_starting_with_a_number:
        return None
    return max(int(re.search(r"\d+", dir_str).group()) for dir_str in dirs_starting_with_a_number)


def load_job_name(job_name, path=".", encoding="json"):
    database_path = Path(path) / "parallel_average_database.json"
    try:
        entry = next(entry for entry in load_database(database_path) if entry["job_name"] == job_name)
    except StopIteration:
        raise EntryDoesNotExist(f"'{job_name}' was not found in {path}")

    if entry.check_result():
        if entry["average_results"] is None:
            return load_collective_result(entry, encoding)
        else:
            return load_averaged_result(entry, encoding)


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
                dict(
                    function_name=function.__name__,
                    args=args,
                    kwargs=kwargs,
                    N_runs=N_runs,
                    average_results=average_results
                ),
                database_path
            )

            if database_path.stat().st_size > 0:
                try:
                    entry = next(entry for entry in load_database(database_path) if entry == new_entry)
                except StopIteration:
                    if action not in (actions.default, actions.do_submit):
                        best_fits_str = ""
                        for best_fit in new_entry.best_fitting_entries_in_database:
                            best_fits_str += str(best_fit) + "\n\n"

                        raise EntryDoesNotExist(
                            f"Best fitting entries in database:\n{best_fits_str}\n"
                            f"Invoked with:\n{new_entry}"
                        )

                if action == actions.print_job_output:
                    return queuing_system_module.print_job_output(
                        parallel_average_path / entry["job_name"]
                    )
                elif action == actions.cancel_job:
                    queuing_system_module.cancel_job(entry["job_name"])
                    entry.remove()
                    cleanup(path=path)
                    return
                elif action == actions.re_submit:
                    entry.check_result()
                    if len(entry.output["successful_runs"]) == volume(entry["N_runs"]):
                        raise ValueError("All runs have finished successfully. No need for re-submitting job.")
                    queuing_system_module.cancel_job(entry["job_name"])
                elif action == actions.default and "entry" not in locals():
                    pass
                elif action == actions.do_submit:
                    try:
                        queuing_system_module.cancel_job(entry["job_name"])
                        entry.remove()
                    except NameError:
                        pass
                else:
                    if not entry.check_result():
                        return

                    if entry["average_results"] is None:
                        return load_collective_result(entry, encoding)
                    else:
                        return load_averaged_result(entry, encoding)

            if action not in (actions.default, actions.do_submit, actions.re_submit):
                raise EntryDoesNotExist()

            assert N_tasks <= volume(N_runs), "'N_tasks' has to be less than or equal to 'N_runs'."
            assert N_tasks >= 1, "'N_tasks' has to be one or greater than one."

            job_index = (largest_existing_job_index(parallel_average_path) or 0) + 1
            job_name = f"{job_index}_{function.__name__}"
            job_path = JobPath(parallel_average_path / job_name)

            if dynamic_load_balancing:
                N_static_runs = setup_dynamic_load_balancing(N_runs, N_tasks, job_path.input_path)
            else:
                N_static_runs = None

            if action == actions.re_submit:
                run_ids_map = prepare_re_submission(entry, job_path, N_tasks)
                num_tasks = len(run_ids_map)
            else:
                run_ids_map = None
                num_tasks = N_tasks

            setup_task_input_data(
                job_name, job_path.input_path, N_runs, num_tasks, average_results, save_interpreter_state,
                dynamic_load_balancing, N_static_runs, keep_runs,
                function, args, kwargs, encoding, run_ids_map
            )

            queuing_system_module.submit(
                num_tasks, job_name, job_path, queuing_system_options
            )

            new_entry.update(dict(
                output=str((job_path / "output.json").relative_to(path)),
                job_name=job_name,
                status="running",
                datetime=datetime.now().isoformat()
            ))
            new_entry.save()

            if action in (actions.do_submit, actions.re_submit):
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


def re_submit(wrapper):
    @wraps(wrapper)
    def f(*args, **kwargs):
        kwargs[action_argname] = actions.re_submit
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
