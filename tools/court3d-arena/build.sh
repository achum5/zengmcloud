#!/bin/sh
# The arena's pieces built in Blender and written into the court's code
# (see stanchion.py, bake.py):
#
#   sh tools/court3d-arena/build.sh [work_dir]
set -e
WORK=${1:-/tmp/court3d-arena}
mkdir -p "$WORK"
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"
blender -b --factory-startup --python "$HERE/stanchion.py" -- "$WORK/stanchion.json" "$WORK/stanchion.blend"
python3 "$HERE/bake.py" "$WORK/stanchion.json" STANCHION src/ui/views/LiveGame/court3d/stanchionModel.ts
