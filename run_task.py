import sys
import numpy as np
import json
import dill
from collections import defaultdict
from json_numpy import NumpyEncoder
from pathlib import Path
from simpleflock import SimpleFlock


task_id = int(sys.argv[1])


with open("../input/run_task_arguments.json", 'r') as f:
    parameters = json.load(f)
    N_runs = parameters["N_runs"]
    N_tasks = parameters["N_tasks"]
    average_arrays = parameters["average_arrays"]
    compute_std = parameters["compute_std"]
    save_interpreter_state = parameters["save_interpreter_state"]
    dynamic_load_balancing = parameters["dynamic_load_balancing"]
    N_static_runs = parameters["N_static_runs"]

with open("../input/function.d", 'rb') as f:
    function = dill.load(f)

with open("../input/args.d", 'rb') as f:
    args = dill.load(f)

with open("../input/kwargs.d", 'rb') as f:
    kwargs = dill.load(f)

if save_interpreter_state:
    dill.load_session("../input/session.sess")

task_result = defaultdict(lambda: 0)
task_square_result = defaultdict(lambda: 0)


def run_ids():
    if dynamic_load_balancing:
        yield from range(task_id - 1, N_static_runs, N_tasks)
        while True:
            with SimpleFlock("../input/chunks_lock"):
                with open("../input/chunks.json", 'r+') as f:
                    chunks = json.load(f)
                    if chunks:
                        chunk = chunks.pop()
                        f.seek(0)
                        json.dump(chunks, f)
                        f.truncate()
                    else:
                        chunk = None

            if chunk is None:
                return

            yield from range(*chunk)
    else:
        yield from range(task_id - 1, N_runs, N_tasks)

N_local_runs = 0

for run_id in run_ids():
    kwargs["run_id"] = run_id
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        with open(f"output_{task_id}.json", 'a') as f:
            json.dump(
                {
                    "failed": True,
                    "run_id": run_id,
                    "error message": type(e).__name__ + ": " + str(e)
                },
                f
            )
        raise e

    if not isinstance(result, (list, tuple)):
        result = [result]

    for i, r in enumerate(result):
        if isinstance(r, np.ndarray) and (average_arrays == 'all' or i in average_arrays):
            task_result[i] += r
            if compute_std == 'all' or (compute_std and i in compute_std):
                task_square_result[i] += r**2
        else:
            task_result[i] = r

    N_local_runs += 1

task_result = [task_result[i] / N_local_runs for i in sorted(task_result)]
task_square_result = {i: r2 / N_local_runs for i, r2 in task_square_result.items()}

with open(f"output_{task_id}.json", 'a') as f:
    json.dump(
        {
            "task_result": task_result,
            "task_square_result": task_square_result,
            "N_local_runs": N_local_runs
        },
        f,
        indent=2,
        cls=NumpyEncoder
    )
