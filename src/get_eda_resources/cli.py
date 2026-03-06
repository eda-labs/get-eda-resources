import json
import logging
import os
import subprocess
import sys
import tarfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import typer
import yaml
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

console = Console()
logger = logging.getLogger("get_eda_resources")


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
    )


def run_kubectl(args: list[str]) -> dict:
    logger.debug("Running command: kubectl %s", " ".join(args))
    result = subprocess.run(
        ["kubectl", *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return json.loads(result.stdout)


def get_eda_crd_resources(group_suffix: str) -> list[str]:
    crds = run_kubectl(["get", "crd", "-o", "json"])
    resources: list[str] = []
    for crd in crds.get("items", []):
        spec = crd.get("spec", {})
        group = spec.get("group")
        if not group or not group.endswith(group_suffix):
            continue
        plural = spec.get("names", {}).get("plural")
        if plural:
            resources.append(f"{plural}.{group}")
    return resources


def write_resources(
    resource: str, namespace: str, out_dir: Path, set_namespace: str | None
) -> tuple[str, int, Path | None]:
    data = run_kubectl(["get", resource, "-n", namespace, "-o", "json"])
    items = data.get("items", [])
    if not items:
        return resource, 0, None

    plural, group = resource.split(".", 1)
    suffix = group.replace("eda.", "", 1)
    output = out_dir / f"{plural}.{suffix}.yaml"

    filtered_items = []
    for item in items:
        metadata = item.get("metadata") or {}
        labels = metadata.get("labels") or {}
        if labels.get("eda.nokia.com/source") == "derived":
            continue

        item.pop("status", None)
        filtered_metadata = {
            "name": metadata.get("name"),
            "namespace": set_namespace or metadata.get("namespace"),
        }
        if labels:
            filtered_metadata["labels"] = labels
        annotations = metadata.get("annotations")
        if annotations:
            filtered_metadata["annotations"] = annotations
        item["metadata"] = filtered_metadata
        filtered_items.append(item)

    if not filtered_items:
        return resource, 0, None

    with output.open("w", encoding="utf-8") as handle:
        for index, item in enumerate(filtered_items):
            if index:
                handle.write("---\n")
            yaml.safe_dump(item, handle, sort_keys=False)

    return resource, len(filtered_items), output


def cli(
    ctx: typer.Context,
    namespace: str = typer.Option("eda", help="Kubernetes namespace."),
    out_dir: Path = typer.Option(
        Path("eda-resources"), help="Output folder for exported files."
    ),
    set_namespace: str | None = typer.Option(
        None, help="Rewrite resource namespaces to this value."
    ),
    group: str = typer.Option(
        "eda.nokia.com", help="CRD group suffix (matches *.group)."
    ),
    archive: bool = typer.Option(
        False, "--archive", help="Create a tar.gz archive of exported resources."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging."),
) -> None:
    if len(sys.argv) == 1:
        console.print(ctx.get_help())
        raise typer.Exit(code=0)

    setup_logging(verbose)

    output_dir = out_dir / namespace
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Exporting resources from namespace '%s'", namespace)

    try:
        resources = get_eda_crd_resources(group)
    except subprocess.CalledProcessError as exc:
        console.print(
            f"[red]kubectl failed while listing CRDs:[/red]\n{exc.stderr.strip()}"
        )
        raise typer.Exit(code=1) from exc
    except json.JSONDecodeError as exc:
        console.print("[red]kubectl did not return valid JSON while listing CRDs.[/red]")
        raise typer.Exit(code=1) from exc

    if not resources:
        console.print(f"[yellow]No CRDs found for group suffix '{group}'.[/yellow]")
        raise typer.Exit(code=0)

    logger.info("Discovered %d resource kinds", len(resources))
    max_workers = os.cpu_count() or 1

    results: list[tuple[str, int, Path | None]] = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for result in executor.map(
                lambda r: write_resources(r, namespace, output_dir, set_namespace),
                resources,
            ):
                results.append(result)
    except subprocess.CalledProcessError as exc:
        console.print(f"[red]kubectl failed while exporting resources:[/red]\n{exc.stderr.strip()}")
        raise typer.Exit(code=1) from exc
    except json.JSONDecodeError as exc:
        console.print("[red]kubectl returned invalid JSON while exporting resources.[/red]")
        raise typer.Exit(code=1) from exc

    written = [(resource, count, path) for resource, count, path in results if path]
    if written:
        table = Table(title="Exported EDA Resources")
        table.add_column("Resource", style="cyan")
        table.add_column("Items", justify="right", style="green")
        table.add_column("File", style="magenta")
        for resource, count, path in sorted(written):
            table.add_row(resource, str(count), str(path))
        console.print(table)
    else:
        console.print("[yellow]No resources written.[/yellow]")

    if archive:
        if any(output_dir.iterdir()):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            archive_name = f"{namespace}-{timestamp}.tar.gz"
            archive_path = out_dir / archive_name
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(output_dir, arcname=namespace)
            console.print(f"[green]Created archive:[/green] {archive_path}")
        else:
            console.print(
                f"[yellow]No resources written to {output_dir}. Skipping archive.[/yellow]"
            )


def main() -> None:
    typer.run(cli)


if __name__ == "__main__":
    main()

