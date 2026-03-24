from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import zipfile

import httpx


def _request_json(client: httpx.Client, url: str) -> dict[str, object]:
    resp = client.get(url)
    resp.raise_for_status()
    return json.loads(resp.text)


def _download(client: httpx.Client, url: str, target: Path) -> None:
    resp = client.get(url)
    resp.raise_for_status()
    target.write_bytes(resp.content)


def _is_successful_workflow_run(
    *,
    client: httpx.Client,
    repo: str,
    workflow_run: dict[str, object],
) -> bool:
    conclusion = workflow_run.get("conclusion")
    if isinstance(conclusion, str):
        return conclusion == "success"
    run_id = workflow_run.get("id")
    if run_id is None:
        return False
    run_payload = _request_json(client, f"https://api.github.com/repos/{repo}/actions/runs/{run_id}")
    return str(run_payload.get("conclusion", "")) == "success"


def _select_baseline_artifact(
    *,
    client: httpx.Client,
    repo: str,
    run_id: str,
    artifacts: list[object],
) -> dict[str, object] | None:
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        if bool(artifact.get("expired", True)):
            continue
        workflow_run = artifact.get("workflow_run")
        if not isinstance(workflow_run, dict):
            continue
        if str(workflow_run.get("head_branch", "")) != "main":
            continue
        if str(workflow_run.get("id", "")) == run_id:
            continue
        if not _is_successful_workflow_run(
            client=client,
            repo=repo,
            workflow_run=workflow_run,
        ):
            continue
        return artifact
    return None


def _download_and_extract(
    *,
    client: httpx.Client,
    download_url: str,
    baseline_dir: Path,
) -> Path:
    archive_path = baseline_dir / "baseline_artifact.zip"
    extract_dir = baseline_dir / "extracted"
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)
    _download(client, download_url, archive_path)
    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(extract_dir)
    return extract_dir


def main() -> None:
    token = os.getenv("GITHUB_TOKEN", "")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    run_id = os.getenv("GITHUB_RUN_ID", "")
    artifact_name = os.getenv("BASELINE_ARTIFACT_NAME", "benchmark-profile-metrics")
    out_dir = Path(os.getenv("VF_PROFILE_OUTPUT_DIR", str(Path.cwd() / "output" / "forager-profiles")))
    baseline_dir = out_dir / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    if not token or not repo:
        print("No GitHub context available; skip baseline download.")
        return

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
    }
    with httpx.Client(headers=headers, timeout=30.0) as client:
        api = f"https://api.github.com/repos/{repo}/actions/artifacts?name={artifact_name}&per_page=100"
        payload = _request_json(client, api)
        artifacts = payload.get("artifacts", [])
        if not isinstance(artifacts, list):
            print("No artifacts payload; skip baseline download.")
            return

        selected = _select_baseline_artifact(
            client=client,
            repo=repo,
            run_id=run_id,
            artifacts=artifacts,
        )
        if selected is None:
            print("No previous benchmark baseline artifact found on main.")
            return

        download_url = str(selected.get("archive_download_url", ""))
        if not download_url:
            print("Selected artifact has no download URL.")
            return

        extract_dir = _download_and_extract(
            client=client,
            download_url=download_url,
            baseline_dir=baseline_dir,
        )

    candidates = list(extract_dir.rglob("profile_metrics.json"))
    if not candidates:
        print("Downloaded baseline artifact does not include profile_metrics.json.")
        return

    baseline_path = baseline_dir / "profile_metrics.json"
    shutil.copy2(candidates[0], baseline_path)
    print(f"Downloaded baseline metrics: {baseline_path}")


if __name__ == "__main__":
    main()
