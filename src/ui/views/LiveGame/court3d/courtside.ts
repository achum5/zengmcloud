import { makeCourtRng } from "../courtRng.ts";
import { project, type Camera } from "./camera.ts";
import { shade } from "./figure.ts";
import {
	BENCH_SEATS,
	benchStart,
	COURT_H,
	COURT_W,
	SEAT_GAP,
	type Side,
} from "./geometry.ts";

// THE COURTSIDE SEATS: two rows of chairs behind each baseline, past the
// photographers on the floor (the front row broken for the basket's
// stanchion), and along the far side behind the table - the media there,
// at their laptops - and past the ends of the benches. Nobody sits right
// behind a bench.
//
// Not players, and nobody looks at them for long: each is a few flat
// shapes in his team's colors (or plain clothes), drawn straight onto the
// picture - no face, no sculpted sprite - so a hundred of them cost next
// to nothing.

export type Folk = {
	x: number;
	y: number;
	// Which way he faces the floor: +x (behind the left baseline), -x, or
	// +y (along the far side).
	facing: "east" | "west" | "north";
	team: Side;
	shirt: string;
	pants: string;
	skin: string;
	hair: string;
	// His size (1 about average).
	size: number;
	media: boolean;
};

export const BASELINE_ROWS = [7.6, 10.4];
export const FAR_ROW = -10.8;
const GAP = 2.5;
const SKINS = ["#f1c7a5", "#d9a77f", "#b07a52", "#8a5a3b", "#5e3b26"];
const HAIRS = [
	"#1b1512",
	"#3b2a1e",
	"#6b4a2c",
	"#a77b45",
	"#d9c08a",
	"#8d8d8d",
];
const PLAIN = [
	"#18181c",
	"#262a33",
	"#3a3a3d",
	"#e8e8e6",
	"#4b5563",
	"#2d2230",
];
const PANTS = ["#2b3a55", "#1c1c20", "#4b4334", "#3b4a6b", "#2e2e33"];

type TeamLike = { colors?: [string, string, string] };

export const courtsideFor = (
	gid: number,
	away: TeamLike | undefined,
	home: TeamLike | undefined,
): Folk[] => {
	const r = makeCourtRng(`courtside|${gid}`);
	const pick = <T>(list: T[]): T => list[Math.floor(r() * list.length)]!;
	const person = (
		x: number,
		y: number,
		facing: Folk["facing"],
		media = false,
	): Folk => {
		const team: Side = r() < 0.78 ? 1 : 0;
		const colors = (team === 1 ? home : away)?.colors;
		const own = colors?.[r() < 0.7 ? 0 : 1] ?? (team ? "#8c1d40" : "#1d3461");
		return {
			x,
			y,
			facing,
			team,
			shirt: media
				? pick(["#18181c", "#262a33", "#1f2937"])
				: r() < 0.35
					? pick(PLAIN)
					: own,
			pants: pick(PANTS),
			skin: pick(SKINS),
			hair: pick(HAIRS),
			size: 0.9 + r() * 0.2,
			media,
		};
	};
	const out: Folk[] = [];
	for (const end of [0, 1] as const) {
		BASELINE_ROWS.forEach((back, row) => {
			for (let y = 0.8; y <= COURT_H - 0.8; y += GAP) {
				// The stanchion's padded base.
				if (row === 0 && y > 20.5 && y < 29.5) {
					continue;
				}
				if (r() < 0.08) {
					continue;
				}
				out.push(
					person(
						end === 0 ? -back : COURT_W + back,
						y + (r() - 0.5) * 0.3,
						end === 0 ? "east" : "west",
					),
				);
			}
		});
	}
	const behindBench = (x: number) =>
		([0, 1] as const).some(
			(t) =>
				x > benchStart(t) - 1 && x < benchStart(t) + BENCH_SEATS * SEAT_GAP + 1,
		);
	for (let x = -3.5; x <= COURT_W + 3.5; x += GAP * 0.92) {
		if (r() < 0.06 || behindBench(x)) {
			continue;
		}
		// Behind the table, the media.
		out.push(person(x + (r() - 0.5) * 0.3, FAR_ROW, "north", x > 36 && x < 58));
	}
	return out;
};

// Seated - or up on his feet (`up`, 0 to 1) - at the size the camera has
// him, with his chair.
export const drawFolk = (
	ctx: CanvasRenderingContext2D,
	cam: Camera,
	p: Folk,
	up: number,
	chair: string,
) => {
	const base = project(cam, { x: p.x, y: p.y, z: 0 });
	const k = base.k * p.size;
	if (
		base.x < -4 * k ||
		base.x > cam.viewW + 4 * k ||
		base.y < -2 * k ||
		base.y > cam.viewH + 7 * k
	) {
		return;
	}
	const side = p.facing === "north" ? 0 : p.facing === "east" ? 1 : -1;
	const X = base.x;
	const Y = base.y;
	const rect = (x: number, y: number, w: number, h: number, c: string) => {
		ctx.fillStyle = c;
		ctx.fillRect(
			Math.round(x),
			Math.round(y),
			Math.max(1, Math.round(w)),
			Math.max(1, Math.round(h)),
		);
	};
	// The chair: its back behind him, its seat under him.
	const backX = X - side * 0.75 * k;
	rect(
		backX - (side ? 0.12 : 0.75) * k,
		Y - 3.2 * k,
		(side ? 0.24 : 1.5) * k,
		3.2 * k,
		shade(chair, -0.35),
	);
	const lift = up * 0.9 * k;
	if (up < 0.5) {
		// Thighs out in front of him, shins down.
		if (side) {
			rect(
				Math.min(X, X + side * 1.2 * k) - 0.1 * k,
				Y - 1.75 * k,
				1.35 * k,
				0.5 * k,
				p.pants,
			);
			rect(
				X + side * 1.0 * k - 0.25 * k,
				Y - 1.5 * k,
				0.5 * k,
				1.5 * k,
				p.pants,
			);
		} else {
			rect(X - 0.6 * k, Y - 1.75 * k, 1.2 * k, 1.75 * k, p.pants);
		}
	} else {
		rect(
			X - (side ? 0.3 : 0.6) * k,
			Y - 2.6 * k,
			(side ? 0.6 : 1.2) * k,
			2.6 * k,
			p.pants,
		);
	}
	// His body, his laptop if he is working, his head.
	const torsoW = (side ? 0.95 : 1.4) * k;
	const torsoTop = Y - (up < 0.5 ? 4.0 : 4.9) * k - lift * 0.2;
	const torsoBot = Y - (up < 0.5 ? 1.6 : 2.5) * k;
	ctx.fillStyle = p.shirt;
	ctx.beginPath();
	ctx.roundRect(X - torsoW / 2, torsoTop, torsoW, torsoBot - torsoTop, 0.3 * k);
	ctx.fill();
	if (p.media && up < 0.5) {
		rect(X - 0.55 * k, Y - 2.3 * k, 1.1 * k, 0.55 * k, "#c9d4e3");
	}
	if (up >= 0.5) {
		// Arms up.
		ctx.strokeStyle = p.skin;
		ctx.lineWidth = Math.max(1, 0.3 * k);
		ctx.beginPath();
		for (const s of [-1, 1]) {
			ctx.moveTo(X + s * torsoW * 0.4, torsoTop + 0.3 * k);
			ctx.lineTo(X + s * (torsoW * 0.4 + 0.5 * k), torsoTop - 1.3 * k);
		}
		ctx.stroke();
	}
	const hr = 0.42 * k;
	const hy = torsoTop - hr * 0.9;
	ctx.fillStyle = p.skin;
	ctx.beginPath();
	ctx.arc(X + side * 0.05 * k, hy, hr, 0, Math.PI * 2);
	ctx.fill();
	ctx.fillStyle = p.hair;
	ctx.beginPath();
	ctx.arc(X - side * 0.08 * k, hy - 0.05 * k, hr * 1.02, Math.PI, Math.PI * 2);
	ctx.fill();
};
