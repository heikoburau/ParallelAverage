from .DatabaseEntry import DatabaseEntry, load_database
from .parallel_average import largest_existing_job_index
import tarfile
import json
from pathlib import Path


def bundle_job(job_name, path=".", compress=True):
    entry = DatabaseEntry.from_job_name(job_name, path)
    job_path = entry.job_path

    entry_path = Path("entry.json")
    with open(entry_path, "w") as f:
        json.dump(entry, f, indent=2)

    with tarfile.open(f"{job_name}.tar", "w:bz2" if compress else "w") as tar:
        tar.add(str(job_path.data_path.resolve()), arcname="data_output")

        if entry.output_path.exists():
            tar.add(str(entry.output_path.resolve()), arcname=entry.output_path.name)

        tar.add(entry_path.name)

    entry_path.unlink()


def unbundle_job(filename, path=".", force=False):
    path = Path(path)
    entry_path = Path("entry.json")

    with tarfile.open(filename, "r") as tar:
        tar.extract(entry_path.name)

    with open(entry_path) as f:
        bundle_entry = json.load(f)
    entry_path.unlink()

    if not force and any(entry == bundle_entry for entry in load_database(path)):
        raise ValueError(
            "[ParallelAverage] Another job with the same function and arguments already exists. "
            "Call with force=True to overwrite the existing entry."
        )

    job_index = (largest_existing_job_index(path / ".parallel_average") or 0) + 1
    new_job_name = f"{job_index}_{bundle_entry['function_name']}"
    new_job_path = path / ".parallel_average" / new_job_name
    bundle_entry["job_name"] = new_job_name
    bundle_entry["output"] = f".parallel_average/{new_job_name}/output.json"

    with tarfile.open(filename, "r") as tar:
        
        import os
        
        def is_within_directory(directory, target):
            
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)
        
            prefix = os.path.commonprefix([abs_directory, abs_target])
            
            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise Exception("Attempted Path Traversal in Tar File")
        
            tar.extractall(path, members, numeric_owner=numeric_owner) 
            
        
        safe_extract(tar, str(new_job_path))

    DatabaseEntry(bundle_entry, path).save()
    print(f"[ParallelAverage] Successfully unbundled job. Added database entry {new_job_name}.")
