import sys
import numpy as np
import json
import dill
from collections import defaultdict


task_id = int(sys.argv[1])


with open("../input/run_task_arguments.json", 'r') as f:
    parameters = json.load(f)
    N_local_runs = parameters["N_local_runs"]
    average_arrays = parameters["average_arrays"]
    save_interpreter_state = parameters["save_interpreter_state"]

with open("../input/function.d", 'rb') as f:
    function = dill.load(f)

with open("../input/args.d", 'rb') as f:
    args = dill.load(f)

with open("../input/kwargs.d", 'rb') as f:
    kwargs = dill.load(f)

if save_interpreter_state:
    dill.load_session("../input/session.sess")

avg_result = defaultdict(lambda: 0)

for n in range(N_local_runs):
    run_id = task_id * N_local_runs + n
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

    if isinstance(result, np.ndarray):
        if average_arrays == 'all' or 0 in average_arrays:
            avg_result[0] += result / N_local_runs
        else:
            avg_result[0] = result
    elif isinstance(result, (tuple, list)):
        for i, r in enumerate(result):
            if isinstance(r, np.ndarray):
                if average_arrays == 'all' or i in average_arrays:
                    avg_result[i] += r / N_local_runs
                else:
                    avg_result[i] = r
    else:
        avg_result[0] = result


result = [avg_result[i] for i in sorted(avg_result)]
if len(result) == 1:
    result = result[0]


if isinstance(result, np.ndarray):
    is_numpy_array = True
    result = result.tolist()
elif isinstance(result, (tuple, list)):
    is_numpy_array = [isinstance(r, np.ndarray) for r in result]
    result = [r.tolist() if isinstance(r, np.ndarray) else r for r in result]
else:
    is_numpy_array = False

with open(f"output_{task_id}.json", 'a') as f:
    json.dump(
        {
            "is_numpy_array": is_numpy_array,
            "result": result
        },
        f
    )
