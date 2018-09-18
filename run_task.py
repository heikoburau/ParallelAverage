import sys
import numpy as np
import json
import dill
import time as time_mod
import random
import threading
from collections import defaultdict
from pathlib import Path
from simpleflock import SimpleFlock


task_id = int(sys.argv[1])

to_be_averaged = lambda i: average_arrays == 'all' or i in average_arrays


with open("../input/run_task_arguments.json", 'r') as f:
    parameters = json.load(f)
    N_runs = parameters["N_runs"]
    N_tasks = parameters["N_tasks"]
    N_threads = parameters["N_threads"]
    average_arrays = parameters["average_arrays"]
    compute_std = parameters["compute_std"]
    save_interpreter_state = parameters["save_interpreter_state"]
    dynamic_load_balancing = parameters["dynamic_load_balancing"]
    N_static_runs = parameters["N_static_runs"]

with open("../input/run_task.d", 'rb') as f:
    run_task = dill.load(f)

function = run_task["function"]
args = run_task["args"]
kwargs = run_task["kwargs"]
encoder = run_task["encoder"]

if save_interpreter_state:
    dill.load_session("../input/session.sess")

def run_ids():
    if dynamic_load_balancing:
        yield from range(task_id - 1, N_static_runs, N_tasks)
        while True:
            chunk = None
            while chunk is None:
                try:
                    with SimpleFlock("../input/chunks_lock"):
                        with open("../input/chunks.json", 'r+') as f:
                            chunks = json.load(f)
                            if not chunks:
                                return

                            chunk = chunks.pop()
                            f.seek(0)
                            json.dump(chunks, f)
                            f.truncate()
                except Exception:
                    time_mod.sleep(0.5 + 0.5 * random.random())
                    chunk = None

            yield from range(*chunk)
    else:
        yield from range(task_id - 1, N_runs, N_tasks)


def execute_run(run_id):
    global task_result, task_square_result, N_local_runs, failed_runs, error_message

    try:
        with open("../progress.txt", "a") as f:
            f.write(str(run_id) + "\n")
    except Exception:
        print("Error while writing to progress.txt")

    kwargs["run_id"] = run_id
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        failed_runs.append(run_id)
        error_message = type(e).__name__ + ": " + str(e)
        return

    if not isinstance(result, (list, tuple)):
        result = [result]

    for i, r in enumerate(result):
        if isinstance(r, np.ndarray) and to_be_averaged(i):
            task_result[i] += r
            if compute_std == 'all' or (compute_std and i in compute_std):
                task_square_result[i] += r**2
        else:
            task_result[i] = r

    N_local_runs += 1


task_result = defaultdict(lambda: 0)
task_square_result = defaultdict(lambda: 0)
N_local_runs = 0
failed_runs = []
error_message = ""
if N_threads > 1:
    active_threads = []

for run_id in run_ids():
    if N_threads > 1:
        while len(active_threads) == N_threads:
            time_mod.sleep(2)
            active_threads = [thread for thread in active_threads if thread.is_alive()]

        thread = threading.Thread(target=execute_run, args=(run_id,))
        thread.start()
        active_threads.append(thread)
    else:
        execute_run(run_id)

if N_threads > 1:
    for thread in active_threads:
        thread.join()

if N_local_runs > 0:
    task_result = [task_result[i] / N_local_runs if to_be_averaged(i) else task_result[i] for i in sorted(task_result)]
    task_square_result = {i: r2 / N_local_runs for i, r2 in task_square_result.items()}
else:
    task_result = None
    task_square_result = None

with open(f"output_{task_id}.json", 'a') as f:
    json.dump(
        {
            "task_result": task_result,
            "task_square_result": task_square_result,
            "N_local_runs": N_local_runs,
            "failed_runs": failed_runs,
            "error_message": {
                "run_id": failed_runs[-1] if failed_runs else -1,
                "message": error_message
            }
        },
        f,
        indent=2,
        cls=encoder
    )
