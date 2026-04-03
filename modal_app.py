import modal

app = modal.App("fetch-promotion-bundle")

image = (
    modal.Image.debian_slim()
    .pip_install(
        "flask",
        "fastapi[standard]",
        "requests",
        "gspread",
        "google-auth",
        "python-dotenv",
    )
    .add_local_dir(".", remote_path="/root")
)


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("fetch-promotion-bundle-secrets")],
    timeout=86400,  # allow up to 24 hours for the job
    schedule=modal.Cron("0 */3 * * *"),
)
def run_job_fn():
    """Runs the job in its own dedicated container with a long timeout."""
    from app import run_job, _lock
    if _lock.acquire(blocking=False):
        run_job()
    else:
        print("Job already running — skipping")


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("fetch-promotion-bundle-secrets")],
)
@modal.fastapi_endpoint(method="POST")
def trigger():
    """POST endpoint to manually trigger the job. Spawns run_job_fn asynchronously."""
    run_job_fn.spawn()
    return {"status": "started", "message": "Job spawned. Check Modal logs for progress."}
