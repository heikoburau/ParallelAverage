import os
import sys
import json
import dill
import time as time_mod
import random
import threading
from collections import defaultdict
from simpleflock import SimpleFlock
from ParallelAverage import WeightedSample
from pathlib import Path


task_id = int(sys.argv[1])
work_dir = Path(sys.argv[2])
job_dir = work_dir.parent
input_dir = job_dir / "input"


to_be_averaged = lambda i: average_results == 'all' or i in average_results


with open(input_dir / "run_task_arguments.json", 'r') as f:
    parameters = json.load(f)
    job_name = parameters["job_name"]
    N_runs = parameters["N_runs"]
    N_tasks = parameters["N_tasks"]
    N_threads = parameters["N_threads"]
    average_results = parameters["average_results"]
    save_interpreter_state = parameters["save_interpreter_state"]
    dynamic_load_balancing = parameters["dynamic_load_balancing"]
    N_static_runs = parameters["N_static_runs"]

with open(input_dir / "run_task.d", 'rb') as f:
    run_task = dill.load(f)

function = run_task["function"]
args = run_task["args"]
kwargs = run_task["kwargs"]
encoder = run_task["encoder"]

os.environ["JOB_NAME"] = job_name

if save_interpreter_state:
    dill.load_session(str(input_dir / "session.sess"))


def run_ids():
    if dynamic_load_balancing:
        yield from range(task_id - 1, N_static_runs, N_tasks)
        while True:
            chunk = None
            while chunk is None:
                try:
                    with SimpleFlock(str(input_dir / "chunks_lock")):
                        with open(input_dir / "chunks.json", 'r+') as f:
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
    global task_result, task_square_result, N_local_runs, local_weights, failed_runs, error_message

    os.environ["RUN_ID"] = str(run_id)
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        failed_runs.append(run_id)
        error_message = type(e).__name__ + ": " + str(e)
        return

    try:
        with open(job_dir / "progress.txt", "a") as f:
            f.write(str(run_id) + "\n")
    except Exception:
        print("Error while writing to progress.txt")

    if not isinstance(result, (list, tuple)):
        result = [result]

    for i, r in enumerate(result):
        if isinstance(r, WeightedSample):
            weight = r.weight
            r = r.sample
        else:
            weight = 1

        local_weights[i] += weight

        if to_be_averaged(i):
            task_result[i] += weight * r if weight > 0 else 0
            task_square_result[i] += weight * abs(r)**2 if weight > 0 else 0
        else:
            task_result[i] = r

    N_local_runs += 1


def finalized_task_result():
    if N_local_runs == 0:
        return None

    return [
        task_result[i] / local_weights[i] if to_be_averaged(i) and local_weights[i] > 0 else task_result[i]
        for i in sorted(task_result)
    ]


def finalized_task_square_result():
    if N_local_runs == 0:
        return None

    return {
        i: (r2 / local_weights[i] if local_weights[i] > 0 else 0)
        for i, r2 in task_square_result.items()
    }


def dump_task_results():
    with open(work_dir / f"output_{task_id}.json", 'w') as f:
        json.dump(
            {
                "task_result": finalized_task_result(),
                "task_square_result": finalized_task_square_result(),
                "N_local_runs": N_local_runs,
                "local_weights": local_weights,
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


task_result = defaultdict(lambda: 0)
task_square_result = defaultdict(lambda: 0)
N_local_runs = 0
local_weights = defaultdict(lambda: 0)
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

    dump_task_results()

if N_threads > 1:
    for thread in active_threads:
        thread.join()
