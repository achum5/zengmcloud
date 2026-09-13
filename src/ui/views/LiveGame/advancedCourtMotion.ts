import type { CourtActor, CourtScene } from "./LiveCourt.tsx";
import { makeCourtRng } from "./courtRng.ts";
import { COURT_H, COURT_W, rimXFor } from "./courtSpots.ts";

export type Point = { x: number; y: number };
export type AdvancedPlayer = CourtActor & {
	t: 0 | 1;
	angle: number;
	displayScale: number;
	stride: number;
	moving: number;
	jump: number;
	shooting: boolean;
	defending: boolean;
};
export type AdvancedBall = Point & {
	z: number;
	owner?: number;
	visible: boolean;
};
export type AdvancedFrame = {
	players: AdvancedPlayer[];
	ball: AdvancedBall;
	progress: number;
	impact?: { x: number; y: number; made: boolean; progress: number };
};
type Track = {
	actor: CourtActor & { t: 0 | 1 };
	from: Point;
	to: Point;
	control: Point;
	angle: number;
	end: number;
};
export type AdvancedPlan = {
	scene: CourtScene;
	tracks: Track[];
	main?: number;
	passer?: number;
	receiver?: number;
	previousBall?: AdvancedBall;
	rim: Point;
	loose: Point;
};

const clamp = (n: number, lo = 0, hi = 1) => Math.max(lo, Math.min(hi, n));
const smooth = (n: number) => {
	const p = clamp(n);
	return p * p * (3 - 2 * p);
};
const mix = (a: number, b: number, p: number) => a + (b - a) * p;
const distance = (a: Point, b: Point) => Math.hypot(a.x - b.x, a.y - b.y);
const pointBetween = (a: Point, b: Point, p: number): Point => ({
	x: mix(a.x, b.x, p),
	y: mix(a.y, b.y, p),
});
const isShot = (scene: CourtScene) =>
	scene.kind === "make" || scene.kind === "miss" || scene.kind === "block";

export const actorTeam = (actor: CourtActor, scene: CourtScene): 0 | 1 =>
	actor.t ??
	(actor.role === "defender" || actor.role === "victim"
		? scene.t === 0
			? 1
			: 0
		: scene.t);

// UI choreography only: the event remains authoritative for every participant
// and outcome. No worker RNG, simulation state, or box-score data is changed.
export const createAdvancedPlan = (
	scene: CourtScene,
	previous?: AdvancedFrame,
): AdvancedPlan => {
	const random = makeCourtRng(`${scene.seed ?? scene.key}|advanced-v1`);
	const seen = new Set<number>();
	const actors = scene.actors
		.filter((a) => {
			if (seen.has(a.pid)) {
				return false;
			}
			seen.add(a.pid);
			return true;
		})
		.map((a) => ({ ...a, t: actorTeam(a, scene) }));
	const main = actors.find((a) => a.role === "main");
	const rim = { x: scene.rimX ?? rimXFor(scene.t), y: 25 };

	// Preserve credited action spots. Only background players yield space, so a
	// made three never migrates inside the arc to satisfy visual separation.
	const targets: Point[] = actors.map((a) => ({ x: a.x, y: a.y }));
	for (let pass = 0; pass < 4; pass++) {
		for (let i = 0; i < actors.length; i++) {
			if (actors[i]!.role !== "onCourt") {
				continue;
			}
			for (let j = 0; j < actors.length; j++) {
				if (i === j) {
					continue;
				}
				const a = targets[i]!;
				const b = targets[j]!;
				const d = distance(a, b);
				if (d >= 3) {
					continue;
				}
				const angle =
					d > 0.01 ? Math.atan2(a.y - b.y, a.x - b.x) : random() * Math.PI * 2;
				a.x = clamp(a.x + Math.cos(angle) * (3 - d), 1.2, COURT_W - 1.2);
				a.y = clamp(a.y + Math.sin(angle) * (3 - d), 1, COURT_H - 1);
			}
		}
	}

	const tracks = actors.map((actor, i): Track => {
		const old = previous?.players.find((p) => p.pid === actor.pid);
		const to = targets[i]!;
		const from = old
			? { x: old.x, y: old.y }
			: actor.role === "in"
				? { x: COURT_W / 2, y: COURT_H + 1 }
				: actor.pid === main?.pid && scene.shooterFrom
					? { ...scene.shooterFrom }
					: { ...to };
		const d = distance(from, to);
		// A gentle bowed route avoids ten players moving as a rigid block. A
		// lane crossing near another player's starting point bends away from it.
		let bend = (random() - 0.5) * Math.min(5, d * 0.22);
		const mid = pointBetween(from, to, 0.5);
		for (const other of previous?.players ?? []) {
			if (other.pid === actor.pid || d < 5 || distance(mid, other) > 4) {
				continue;
			}
			const cross =
				(to.x - from.x) * (other.y - mid.y) -
				(to.y - from.y) * (other.x - mid.x);
			bend = (cross >= 0 ? -1 : 1) * Math.min(5, d * 0.25);
			break;
		}
		const control = {
			x: clamp(mid.x - ((to.y - from.y) / Math.max(1, d)) * bend, 0, COURT_W),
			y: clamp(
				mid.y + ((to.x - from.x) / Math.max(1, d)) * bend,
				-1,
				COURT_H + 1,
			),
		};
		return {
			actor,
			from,
			to,
			control,
			angle: old?.angle ?? Math.atan2(rim.y - to.y, rim.x - to.x),
			// Shooters gather before release; teammates have the rest of the beat.
			end:
				isShot(scene) && actor.pid === main?.pid ? 0.24 : 0.7 + random() * 0.15,
		};
	});
	const nearest = (spot: Point | undefined, exclude?: number) => {
		if (!spot) {
			return undefined;
		}
		return tracks
			.filter(
				(t) =>
					t.actor.t === scene.t &&
					t.actor.pid !== exclude &&
					t.actor.role !== "out",
			)
			.sort((a, b) => distance(a.to, spot) - distance(b.to, spot))[0]?.actor
			.pid;
	};
	return {
		scene,
		tracks,
		main: main?.pid,
		passer:
			actors.find((a) => a.role === "assist")?.pid ??
			(scene.passFrom ? nearest(scene.passFrom, main?.pid) : undefined),
		receiver: nearest(scene.ballTo),
		previousBall: previous?.ball.visible ? { ...previous.ball } : undefined,
		rim,
		loose: {
			x: clamp(rim.x + (rim.x < 47 ? 1 : -1) * (3 + random() * 3), 1, 93),
			y: 25 + (random() - 0.5) * 9,
		},
	};
};

// The same hand position drives the drawn arm and ball. In particular, the
// ball follows the CURRENT moving player, not the destination of his path.
export const playerHand = (player: AdvancedPlayer): Point & { z: number } => {
	const scale = player.displayScale;
	const direction = Math.cos(player.angle) >= 0 ? 1 : -1;
	return {
		x: player.x + direction * (player.shooting ? 0.45 : 1.15) * scale,
		y: player.y - (player.shooting ? 0.1 : 0) * scale,
		z: ((player.shooting ? 5.7 : 2.6) + player.jump) * scale,
	};
};

export const sampleAdvancedPlan = (
	plan: AdvancedPlan,
	progress: number,
	displayScale = 1,
): AdvancedFrame => {
	const p = clamp(Number.isFinite(progress) ? progress : 1);
	const { scene } = plan;
	const shot = isShot(scene);
	const players: AdvancedPlayer[] = plan.tracks.map((track) => {
		const u = smooth(p / track.end);
		const v = 1 - u;
		const d = distance(track.from, track.to);
		const x =
			v * v * track.from.x + 2 * v * u * track.control.x + u * u * track.to.x;
		const y =
			v * v * track.from.y + 2 * v * u * track.control.y + u * u * track.to.y;
		const moving =
			p < track.end
				? Math.min(1, d / 5) * Math.sin(Math.PI * clamp(p / track.end))
				: 0;
		const defending =
			track.actor.t !== scene.t || track.actor.role === "defender";
		const target =
			moving > 0.15 && !defending ? track.to : (scene.ballFrom ?? plan.rim);
		const desired = Math.atan2(target.y - y, target.x - x);
		const delta = Math.atan2(
			Math.sin(desired - track.angle),
			Math.cos(desired - track.angle),
		);
		const shooting =
			shot && track.actor.pid === plan.main && p > 0.15 && p < 0.64;
		const contest =
			shot &&
			scene.zone !== "ft" &&
			defending &&
			distance(track.to, scene.ballFrom ?? plan.rim) < 5;
		const jumping =
			shooting ||
			contest ||
			(scene.kind === "reb" && track.actor.pid === plan.main) ||
			(scene.kind === "jump" && track.actor.role !== "onCourt");
		const jumpPhase = clamp((p - 0.14) / 0.5);
		return {
			...track.actor,
			displayScale,
			x,
			y,
			angle: track.angle + delta * smooth(p / 0.3),
			stride: Math.sin(d * u * 2.4 + track.actor.pid) * moving,
			moving,
			jump: jumping
				? Math.sin(Math.PI * jumpPhase) * (scene.zone === "ft" ? 0.25 : 1.4)
				: 0,
			shooting: shooting || contest,
			defending: defending && scene.kind !== "dead" && scene.kind !== "sub",
		};
	});
	const player = (pid: number | undefined) =>
		players.find((a) => a.pid === pid);
	const main = player(plan.main);
	const ownerBall = (
		owner: AdvancedPlayer | undefined,
		dribble = false,
	): AdvancedBall => {
		if (!owner) {
			return { x: 47, y: 25, z: 0, visible: false };
		}
		const hand = playerHand(owner);
		return {
			...hand,
			z:
				dribble && !owner.shooting
					? 0.35 + Math.abs(Math.cos(p * Math.PI * 5)) * 2.25
					: hand.z,
			owner: owner.pid,
			visible: true,
		};
	};
	const fly = (
		from: Point & { z?: number },
		to: Point & { z?: number },
		fraction: number,
		height: number,
	): AdvancedBall => {
		const u = clamp(fraction);
		return {
			...pointBetween(from, to, u),
			z: mix(from.z ?? 2.6, to.z ?? 2.6, u) + Math.sin(Math.PI * u) * height,
			visible: true,
		};
	};
	let ball: AdvancedBall = { x: 47, y: 25, z: 0, visible: false };
	let impact: AdvancedFrame["impact"];
	if (scene.kind === "attempt") {
		const passer = player(plan.passer);
		if (passer && main && p < 0.66) {
			ball =
				p < 0.15
					? ownerBall(passer)
					: fly(playerHand(passer), playerHand(main), (p - 0.15) / 0.51, 0.7);
		} else {
			ball = ownerBall(main, true);
		}
	} else if (shot && main) {
		const release = 0.3;
		const arrival = scene.kind === "block" ? 0.49 : 0.77;
		const releaseFrame =
			p < release
				? main
				: {
						...main,
						x: plan.tracks.find((t) => t.actor.pid === main.pid)!.to.x,
						y: plan.tracks.find((t) => t.actor.pid === main.pid)!.to.y,
						shooting: true,
						jump: scene.zone === "ft" ? 0.2 : 1.2,
					};
		const source = playerHand(releaseFrame);
		const blocker = players.find((a) => a.role === "defender");
		const target =
			scene.kind === "block" && blocker
				? { ...playerHand(blocker), z: 7 }
				: { ...plan.rim, z: 6 };
		if (p < release) {
			ball = ownerBall(main);
		} else if (p < arrival) {
			ball = fly(
				source,
				target,
				(p - release) / (arrival - release),
				scene.zone === "atRim" ? 1 : 4.5,
			);
		} else {
			const after = (p - arrival) / (1 - arrival);
			ball =
				scene.kind === "make"
					? fly(target, { ...plan.rim, z: 0.3 }, after, 0)
					: fly(target, { ...plan.loose, z: 0.5 }, after, 1.2);
			impact = { ...target, made: scene.kind === "make", progress: after };
		}
	} else if (scene.kind === "advance" || scene.kind === "swing") {
		const receiver = player(plan.receiver);
		const previousOwner = player(plan.previousBall?.owner);
		if (
			receiver &&
			previousOwner &&
			receiver.pid !== previousOwner.pid &&
			p < 0.5
		) {
			ball = fly(playerHand(previousOwner), playerHand(receiver), p / 0.5, 1);
		} else if (receiver && !previousOwner && plan.previousBall && p < 0.25) {
			ball = fly(plan.previousBall, playerHand(receiver), p / 0.25, 1.1);
		} else {
			ball = ownerBall(receiver, true);
		}
	} else if (scene.kind === "reb" && main) {
		ball =
			p < 0.58
				? fly(
						plan.previousBall ?? { ...(scene.ballFrom ?? plan.rim), z: 6 },
						playerHand(main),
						p / 0.58,
						2,
					)
				: ownerBall(main);
	} else if (scene.kind === "stl" && main) {
		const victim = players.find((a) => a.role === "victim");
		ball =
			p < 0.22
				? ownerBall(victim)
				: p < 0.62
					? fly(
							victim ? playerHand(victim) : (scene.ballFrom ?? main),
							playerHand(main),
							(p - 0.22) / 0.4,
							-1,
						)
					: ownerBall(main, true);
	} else if (scene.kind === "oob" || scene.kind === "tov") {
		const from = plan.previousBall ??
			scene.ballFrom ??
			main ?? { x: 47, y: 25 };
		const to = scene.ballTo ?? { x: from.x + 3, y: from.y < 25 ? -1 : 51 };
		ball = fly(from, { ...to, z: 0.2 }, smooth(p), 1);
	} else if (scene.kind === "jump") {
		const center = scene.ballFrom ?? { x: 47, y: 25 };
		ball =
			p < 0.5
				? { ...center, z: 2 + Math.sin(Math.PI * p) * 7, visible: true }
				: fly(
						{ ...center, z: 9 },
						{ ...(scene.ballTo ?? center), z: 2.6 },
						(p - 0.5) * 2,
						0,
					);
	} else if (scene.kind === "foul") {
		ball = ownerBall(players.find((a) => a.role === "victim") ?? main);
	}
	return { players, ball, progress: p, impact };
};
