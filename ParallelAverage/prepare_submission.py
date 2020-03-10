import __main__ as _main_module
import dill
import pickle
import json


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
    encoding,
    run_ids_map
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
                "encoding": encoding,
                "new_task_ids": list(run_ids_map) if run_ids_map is not None else None,
                "run_ids_map": run_ids_map
            },
            f,
            indent=2
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


def volume(x):
    if isinstance(x, int):
        return x

    result = 1
    for x_i in x:
        result *= x_i
    return result
