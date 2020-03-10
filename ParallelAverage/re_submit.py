from .gathering import Gatherer
import shutil


def prepare_re_submission(old_database_entry, new_job_path, num_new_tasks):
    """
    initializes `new_job_path` with successful runs from `old_database_entry` and
    returns a dict mapping new task-ids to a list of assigned run-ids respectively
    """
    total_task = Gatherer(old_database_entry).run().total_task

    run_ids = sorted(
        set(range(old_database_entry["N_runs"])) -
        {eval(run_id) for run_id in total_task.successful_runs}
    )
    total_task.failed_runs = []
    total_task.error_message = {}
    total_task.done = True
    total_task.save(new_job_path.data_path / "1_task_output.json")

    for raw_results in total_task.raw_results_files:
        shutil.copy(raw_results, new_job_path.data_path / raw_results.name)

    task_base = max(new_job_path.task_ids) + 1
    num_new_tasks = min(num_new_tasks, len(run_ids))

    return {
        task_base + i: [run_ids[j] for j in range(i, len(run_ids), num_new_tasks)]
        for i in range(num_new_tasks)
    }
