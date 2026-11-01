#!/usr/bin/env bash
set -euo pipefail

# Run on c0 as ubuntu. Existing Spark and system Python stay separate.
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
install_dir="$HOME/6893/duckdb"
venv_dir="$HOME/6893/.venv-duckdb"
data_dir="${RADAR_DATA_DIR:-$HOME/6893/data}"
runtime=/opt/6893/python/bin/python3.11

if [[ "$(uname -m)" != aarch64 || "$(uname -s)" != Linux ]]; then
  printf '%s\n' 'This installer targets the ARM Linux c0 VM.' >&2
  exit 2
fi
[[ -x "$runtime" ]] || { printf 'Missing Python runtime: %s\n' "$runtime" >&2; exit 2; }
data_dir="$("$runtime" -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).expanduser().resolve())' "$data_dir")"
mkdir -p "$install_dir" "$data_dir" "$data_dir/raw" "$data_dir/processed" "$data_dir/published" "$data_dir/tmp"
if [[ "$script_dir" != "$install_dir" ]]; then
  for name in query.py setup.sh requirements-c0.txt verify_duckdb.py; do
    install -m 644 "$script_dir/$name" "$install_dir/$name"
  done
fi
"$runtime" -m venv "$venv_dir"
"$venv_dir/bin/python" -m pip install --index-url https://pypi.org/simple \
  --only-binary=:all: --require-hashes --no-cache-dir -r "$install_dir/requirements-c0.txt"
"$venv_dir/bin/python" "$install_dir/query.py" --data-dir "$data_dir" --init

wrapper="$(mktemp)"
trap 'rm -f "$wrapper"' EXIT
{
  printf '#!/usr/bin/env bash\n'
  printf 'default_data_dir=%q\n' "$data_dir"
  printf 'export RADAR_DATA_DIR="${RADAR_DATA_DIR:-$default_data_dir}"\n'
  printf 'exec %q %q "$@"\n' "$venv_dir/bin/python" "$install_dir/query.py"
} > "$wrapper"
sudo -n install -m 755 "$wrapper" /usr/local/bin/6893-duckdb
"$venv_dir/bin/python" "$install_dir/verify_duckdb.py" --data-dir "$data_dir"
printf '%s\n' 'DuckDB ready. Run: 6893-duckdb "SELECT version();"'
