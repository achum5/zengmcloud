// A LOOK AT THE CAST, not an assertion about it. Faces are art as much as
// model; the only way to judge whether a press room looks like a room full of
// people is to render one and look.
//
// Skipped unless SOCIAL_FACES_OUT is set, like the aging preview and the recap
// corpus, so CI pays nothing.
//
//   SPORT=basketball SOCIAL_FACES_OUT=/tmp/cast.html \
//     npx vitest run --project basketball src/worker/util/socialFacesPreview.test.ts

import { faceToSvgString, svgs } from "facesjs";
import { describe, test } from "vitest";
import { CIVILIAN_CLOTHES } from "../../common/civilianClothes.ts";
import { socialAccountPicture, socialFace, roleFor } from "./socialFaces.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const OUT = nodeEnv.SOCIAL_FACES_OUT;

const ARCHETYPES = [
	"insider",
	"nationalPundit",
	"beatWriter",
	"analytics",
	"capNerd",
	"draftHead",
	"historian",
	"localRadio",
	"aggregator",
	"troll",
	"homerFan",
	"doomerFan",
	"casualFan",
];

const TEAM: [string, string, string] = ["#0c2340", "#c8102e", "#ffffff"];

describe.runIf(OUT)("the cast", () => {
	test("renders", async () => {
		// The clothing has to be in the table before anything is drawn - the UI
		// does this at import time, see civilianJersey.ts.
		for (const [id, svg] of Object.entries(CIVILIAN_CLOTHES)) {
			(svgs.jersey as unknown as Record<string, string>)[id] = svg;
		}

		const perRole = Number(nodeEnv.SOCIAL_FACES_N ?? 6);
		let out = "";
		for (const archetypeId of ARCHETYPES) {
			const role = roleFor(archetypeId);
			let cells = "";
			for (let i = 0; i < perRole; i++) {
				const id = `m:${archetypeId}-${i}`;
				const picture = socialAccountPicture(id, archetypeId, TEAM);
				const { age, gender } = socialFace(id, archetypeId);
				const svg = faceToSvgString(picture.face as any, {
					teamColors: picture.colors,
					jersey: { id: picture.jersey },
				} as any);
				cells += `<div class=c><div class=ring><div class=in>${svg}</div></div><div class=full>${svg}</div><div class=l>${gender[0]}${age} · ${(picture.jersey ?? "").replace("civ-", "")}</div></div>`;
			}
			out += `<section><h2>${archetypeId} <span>ages ${role.age[0]}-${role.age[1]}</span></h2><div class=row>${cells}</div></section>`;
		}

		const html = `<html><head><meta charset=utf-8><style>
body{background:#fff;font:12px system-ui;margin:0;padding:14px;color:#111}
h1{font:600 16px system-ui;margin:0 0 12px}
h2{font:600 13px system-ui;margin:16px 0 6px}
h2 span{font-weight:400;color:#777}
.row{display:flex;gap:10px;flex-wrap:wrap}
.c{width:104px;text-align:center}
.ring{width:72px;height:72px;border-radius:50%;overflow:hidden;position:relative;background:#e9ecef;margin:0 auto}
.in{position:absolute;width:65.45px;height:98.18px;left:3.27px;top:-20.45px}
.in svg{width:100%;height:100%;display:block}
.full{width:104px;height:156px;overflow:hidden;margin-top:4px}
.full svg{width:104px;height:156px;display:block}
.l{font-size:10px;color:#555;margin-top:2px}
</style></head><body><h1>The cast — avatar crop above, full frame below</h1>${out}</body></html>`;

		const fs = await import(("node" + ":fs") as any);
		fs.writeFileSync(OUT!, html);
	});
});
