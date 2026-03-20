import importlib.metadata
import json
import logging
import os
import shutil
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

DISTRIBUTION_NAME = "get-eda-resources"
VERSION_FALLBACK = "0.1.0"


def _package_version() -> str:
    try:
        return importlib.metadata.version(DISTRIBUTION_NAME)
    except importlib.metadata.PackageNotFoundError:
        return VERSION_FALLBACK


def _print_version() -> None:
    console.print(f"version: [bold]{_package_version()}[/bold]")


def _version_flag_callback(value: bool) -> None:
    if value:
        _print_version()
        raise typer.Exit()


# (apiGroup, kind) — excludes GVK from export (no kubectl get / no file).
SKIP_GVKS: frozenset[tuple[str, str]] = frozenset(
    {("core.eda.nokia.com", "WaitForInput")}
)


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
        kind = spec.get("names", {}).get("kind")
        if kind and (group, kind) in SKIP_GVKS:
            crd_name = (crd.get("metadata") or {}).get(
                "name", f"{spec.get('names', {}).get('plural', '?')}.{group}"
            )
            logger.debug("Skipping excluded CRD: %s (%s/%s)", crd_name, group, kind)
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


TOPONODE_FILE_NAME = "toponodes.core.nokia.com.yaml"


def _is_toponode(doc: object) -> bool:
    if not isinstance(doc, dict):
        return False
    if doc.get("kind") != "TopoNode":
        return False
    api = str(doc.get("apiVersion") or "")
    return "core.eda.nokia.com" in api


def _apply_toponode_cx_mode(doc: dict) -> None:
    spec = doc.setdefault("spec", {})
    if not isinstance(spec, dict):
        return

    metadata = doc.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        doc["metadata"] = metadata
    annotations = metadata.setdefault("annotations", {})
    if not isinstance(annotations, dict):
        annotations = {}
        metadata["annotations"] = annotations

    for key in ("macAddress", "serialNumber"):
        if key not in spec:
            continue
        value = spec.pop(key)
        annotations[key] = "" if value is None else str(value)

    spec["onBoarded"] = False
    spec.pop("productionAddress", None)


def _effective_namespace(directory: Path, explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    if directory.is_dir():
        subdirs = [
            p.name
            for p in directory.iterdir()
            if p.is_dir() and not p.name.startswith(".")
        ]
        if len(subdirs) == 1:
            return subdirs[0]
    return "eda"


def _resolve_toponode_path(
    directory: Path, namespace: str, file_override: Path | None
) -> Path:
    if file_override is not None:
        return file_override
    return directory / namespace / TOPONODE_FILE_NAME


def _write_yaml_documents(path: Path, documents: list[object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for index, doc in enumerate(documents):
            if index:
                handle.write("---\n")
            yaml.safe_dump(doc, handle, sort_keys=False)


def _run_toponode_cx_mode(path: Path) -> int:
    if not path.is_file():
        console.print(
            f"[red]File not found:[/red] {path}\n"
            "Export resources first or pass [cyan]--file[/cyan] / adjust "
            "[cyan]-d[/cyan] and [cyan]--namespace[/cyan]."
        )
        raise typer.Exit(code=1)

    text = path.read_text(encoding="utf-8")
    documents = [d for d in yaml.safe_load_all(text) if d is not None]
    if not documents:
        console.print(f"[yellow]No YAML documents in {path}.[/yellow]")
        raise typer.Exit(code=0)

    changed = 0
    for doc in documents:
        if _is_toponode(doc):
            _apply_toponode_cx_mode(doc)
            changed += 1

    if changed == 0:
        console.print(
            f"[yellow]No TopoNode resources found in {path}; file not modified.[/yellow]"
        )
        raise typer.Exit(code=0)

    _write_yaml_documents(path, documents)
    console.print(
        f"[green]Updated[/green] {changed} TopoNode(s) in [cyan]{path}[/cyan] "
        "([bold]onBoarded: false[/bold], removed [bold]productionAddress[/bold], "
        "[bold]macAddress[/bold] / [bold]serialNumber[/bold] moved to annotations)."
    )
    return changed


edit_app = typer.Typer(
    help="Edit exported EDA resource files.",
    invoke_without_command=True,
)


@edit_app.callback()
def edit_callback(
    ctx: typer.Context,
    directory: Path = typer.Option(
        Path("eda-resources"),
        "-d",
        "--directory",
        help="Directory containing exported resources.",
    ),
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["directory"] = directory
    if ctx.invoked_subcommand is None:
        console.print(ctx.get_help())
        raise typer.Exit(code=0)


@edit_app.command("toponode")
def edit_toponode(
    ctx: typer.Context,
    cx_mode: bool = typer.Option(
        False,
        "--cx-mode",
        help=(
            "CX mode: onBoarded false; remove productionAddress; move macAddress and "
            "serialNumber from spec to metadata.annotations."
        ),
    ),
    namespace: str | None = typer.Option(
        None,
        "--namespace",
        "-n",
        help=(
            "Namespace subfolder under the export directory. "
            "If omitted and the directory has exactly one subfolder, that name is used; "
            "otherwise default is 'eda'."
        ),
    ),
    file: Path | None = typer.Option(
        None,
        "--file",
        "-f",
        help="Explicit path to toponodes YAML (overrides -d / --namespace).",
    ),
) -> None:
    """Edit exported TopoNode resources."""
    if not cx_mode:
        console.print(
            "[yellow]No changes made.[/yellow] Pass [cyan]--cx-mode[/cyan] to apply "
            "TopoNode CX transforms."
        )
        raise typer.Exit(code=1)

    directory = ctx.obj.get("directory", Path("eda-resources"))
    ns = _effective_namespace(directory, namespace)
    path = _resolve_toponode_path(directory, ns, file)
    _run_toponode_cx_mode(path)


app = typer.Typer(
    help="Export Nokia EDA Kubernetes resources from a namespace.",
    invoke_without_command=True,
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.callback()
def default_export(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show version and exit.",
        callback=_version_flag_callback,
        is_eager=True,
    ),
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
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable debug logging."
    ),
) -> None:
    if len(sys.argv) == 1:
        console.print(ctx.get_help())
        raise typer.Exit(code=0)

    if ctx.invoked_subcommand is not None:
        return

    setup_logging(verbose)

    if shutil.which("kubectl") is None:
        console.print(
            "[red]Required binary not found:[/red] `kubectl` is not in PATH.\n"
            "Install kubectl and ensure it is available in your shell PATH."
        )
        raise typer.Exit(code=1)

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
        console.print(
            "[red]kubectl did not return valid JSON while listing CRDs.[/red]"
        )
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
        console.print(
            f"[red]kubectl failed while exporting resources:[/red]\n{exc.stderr.strip()}"
        )
        raise typer.Exit(code=1) from exc
    except json.JSONDecodeError as exc:
        console.print(
            "[red]kubectl returned invalid JSON while exporting resources.[/red]"
        )
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


@app.command("version")
def version_command() -> None:
    """Print the installed tool version."""
    _print_version()


app.add_typer(edit_app, name="edit")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
