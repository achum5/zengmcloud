#!/bin/sh
# The players' body, built in Blender from the court's Classic player and
# baked for the court (see classic.py, weights.py, bake.py):
#
#   sh tools/court3d-body/build.sh [work_dir]
set -e
WORK=${1:-/tmp/court3d-body}
mkdir -p "$WORK/out"
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"
COURT3D_BODY_OUT="$WORK/masses.json" npx vitest run --project basketball src/ui/views/LiveGame/court3d/meshMasses.test.ts
blender -b --factory-startup --python "$HERE/classic.py" -- "$WORK/masses.json" "$WORK/out" "${FACES:-6000}"
blender -b --factory-startup --python "$HERE/weights.py" -- "$WORK/masses.json" "$WORK/out/classic.blend" "$WORK/out/weights.npz"
python3 "$HERE/bake.py" "$WORK/masses.json" "$WORK/out" src/ui/views/LiveGame/court3d/bodyMesh.data.ts
