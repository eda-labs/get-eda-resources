# Get EDA Resources

`get-eda-resources` tool (alias `ger`) exports Nokia EDA resources using the Kubernetes API (`kubectl` required).
It can be used to copy EDA resources between namespaces or clusters without taking a full EDA backup.

## Installation

```bash
curl eda.dev/uvx | sh -s -- \
https://github.com/eda-labs/get-eda-resources/archive/refs/heads/main.zip
```

After installation, the commands are available as:

```bash
get-eda-resources --help
ger --help
```

## Usage

```bash
get-eda-resources --namespace <namespace>
```

> Derived resources (`eda.nokia.com/source=derived`) are skipped.

Exported files are written to `eda-resources/<namespace>` by default.

### Options

- `--namespace`: Kubernetes namespace to export from (default: `eda`).
- `--out-dir`: Directory where exported resources are written (default: `eda-resources`).
- `--archive`: Create a tar.gz archive of exported resources.
- `--set-namespace`: Rewrite exported resource namespaces to this value.
- `--group`: CRD group suffix to match (default: `eda.nokia.com`).
- `--verbose`, `-v`: Enable debug logging.

## Applying Fetched Resources

To apply exported resources to another cluster or namespace, you can use the
[etc script](https://github.com/eda-labs/etc-script/) until `edactl` is available
as a native binary.
