# A TOOL, not part of the game: an arena piece built in Blender (see
# stanchion.py) written into the court's code as data.
#
#   python3 tools/court3d-arena/bake.py stanchion.json STANCHION \
#     src/ui/views/LiveGame/court3d/stanchionModel.ts
import json
import sys

SRC, NAME, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
D = json.load(open(SRC))
with open(OUT, "w") as o:
    o.write("// BUILT IN BLENDER by tools/court3d-arena (see arenaModel.ts for what it\n")
    o.write("// is). Do not edit: build it again.\n")
    o.write('import type { ArenaModel } from "./arenaModel.ts";\n\n')
    o.write(f"export const {NAME}: ArenaModel = {json.dumps(D, separators=(',', ':'))};\n")
print("wrote", OUT)
