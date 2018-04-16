import sys
import numpy as np
import json
import dill
from collections import defaultdict
from json_numpy import NumpyEncoder


task_id = int(sys.argv[1])


with open("../input/run_task_arguments.json", 'r') as f:
    parameters = json.load(f)
    N_runs = parameters["N_runs"]
    N_tasks = parameters["N_tasks"]
    average_arrays = parameters["average_arrays"]
    compute_std = parameters["compute_std"]
    save_interpreter_state = parameters["save_interpreter_state"]

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

N_local_runs = N_runs // N_tasks
if N_runs % N_tasks != 0:
    N_local_runs += 1 if (task_id - 1) < N_runs % N_tasks else 0

for n in range(N_local_runs):
    run_id = n * N_tasks + (task_id - 1)
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
            task_result[i] += r / N_local_runs
            if compute_std == 'all' or (compute_std and i in compute_std):
                task_square_result[i] += r**2 / N_local_runs
            continue

        task_result[i] = r

task_result = [task_result[i] for i in sorted(task_result)]

with open(f"output_{task_id}.json", 'a') as f:
    json.dump(
        {
            "task_result": task_result,
            "task_square_result": dict(task_square_result)
        },
        f,
        cls=NumpyEncoder
    )
