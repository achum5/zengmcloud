import { useEffect, useMemo, useRef, useState } from "react";
import type { HeadlineSceneSpec } from "../../../../common/headlineScene.ts";
import { toWorker } from "../../../util/toWorker.ts";
import type { headlineScene } from "../../../../worker/util/headlineScene.ts";
import type { Authored } from "./authored.ts";
import { capOf } from "./cap.ts";
import { cappedHead } from "./capHead.ts";
import { loadHead, type HeadSprite } from "./faces.ts";
import Court3D from "./Court3D.tsx";
import {
	DraftBroadcast,
	makeDraftFeed,
	PRE_ROLL_MS,
} from "./DraftBroadcast.tsx";
import { crowdFaces, type CrowdFace } from "./crowdFaces.ts";
import { ASK_AGAIN_MS } from "./draftClock.ts";
import { stageDraftNight } from "./draftStage.ts";
import { draftVenue, paintDraftVenue } from "./draftVenue.ts";
import { draftLook } from "./draftDress.ts";
import type { Look } from "./figure.ts";
import { logoPicture } from "./logoPicture.ts";

// THE SCENE ITSELF (see HeadlineScene.tsx, which fetches this only once it
// is wanted): what it is made from asked of the worker, staged, and played
// by the court - in the building of the team it is about, dressed for the
// draft (see draftVenue.ts).

type Made = NonNullable<Awaited<ReturnType<typeof headlineScene>>>;

const noop = () => {};

const HeadlineStage = ({
	scene,
	playing,
	onEnd,
	onNone,
}: {
	scene: HeadlineSceneSpec;
	playing: boolean;
	onEnd?: () => void;
	onNone: () => void;
}) => {
	// (By what it says, not which object says it: the page may hand over a
	// new one each time it draws.)
	const { type, pid: scenePid } = scene;
	const [made, setMade] = useState<Made>();
	const onNoneRef = useRef(onNone);
	onNoneRef.current = onNone;
	useEffect(() => {
		let alive = true;
		let timer: ReturnType<typeof setTimeout> | undefined;
		// (Asked again a few times before it is given up on - see ClockStage.)
		const ask = async (i: number) => {
			let got: Made | undefined;
			let failed = false;
			try {
				got =
					(await toWorker("main", "getHeadlineScene", {
						type,
						pid: scenePid,
					})) ?? undefined;
			} catch (error) {
				failed = true;
				console.warn("Draft scene", error);
			}
			if (!alive) {
				return;
			}
			if (got) {
				setMade(got);
			} else if (failed && i + 1 < ASK_AGAIN_MS.length) {
				timer = setTimeout(
					() => {
						void ask(i + 1);
					},
					ASK_AGAIN_MS[i + 1],
				);
			} else {
				onNoneRef.current();
			}
		};
		void ask(0);
		return () => {
			alive = false;
			clearTimeout(timer);
		};
	}, [type, scenePid]);

	// The team's logo, for the front of the cap - asked for once - and big,
	// for the LED wall.
	const logo = made?.team.imgURLSmall ?? made?.team.imgURL;
	const crest = useMemo(() => logoPicture(logo, 96), [logo]);
	const bigLogo = made?.team.imgURL ?? made?.team.imgURLSmall;
	const [wallLogo, setWallLogo] = useState<HTMLCanvasElement>();
	useEffect(() => {
		let alive = true;
		void logoPicture(bigLogo, 360).then((c) => {
			if (alive) {
				setWallLogo(c);
			}
		});
		return () => {
			alive = false;
		};
	}, [bigLogo]);
	// The faces round the green room's tables.
	const [faces, setFaces] = useState<CrowdFace[]>();
	useEffect(() => {
		let alive = true;
		void crowdFaces().then((f) => {
			if (alive && f.length > 0) {
				setFaces(f);
			}
		});
		return () => {
			alive = false;
		};
	}, []);

	// Over, it plays again from the top the next time it is asked to play.
	const [run, setRun] = useState(0);
	// The podium read out yet (see below).
	const [preDone, setPreDone] = useState(false);
	const over = useRef(false);
	const onEndRef = useRef(onEnd);
	onEndRef.current = onEnd;
	useEffect(() => {
		if (playing && over.current) {
			over.current = false;
			setRun((n) => n + 1);
			setPreDone(false);
		}
	}, [playing]);

	// THE PICK IS IN FIRST (see DraftBroadcast.tsx), over the team's colors -
	// covering the room while he is got ready - and only then the room.
	useEffect(() => {
		if (!made || preDone || !playing) {
			return;
		}
		const timer = setTimeout(() => {
			setPreDone(true);
		}, PRE_ROLL_MS);
		return () => {
			clearTimeout(timer);
		};
	}, [made, preDone, playing]);
	const feed = useMemo(() => makeDraftFeed(), []);

	// Not seen till his head is drawn, the cap on it - nor does it start: a
	// plain head with no cap on it would be on its way out otherwise. (Or
	// till it is plain that it never will be.)
	const [dressed, setDressed] = useState(false);
	useEffect(() => {
		if (!made) {
			return;
		}
		const timer = setTimeout(() => {
			setDressed(true);
		}, 4000);
		return () => {
			clearTimeout(timer);
		};
	}, [made]);

	const pid = made?.player.pid;
	const colors = made?.team.colors;
	const race = made?.player.race;
	const staged = useMemo(
		() => (pid === undefined ? undefined : stageDraftNight(pid, race)),
		[pid, race],
	);
	// The room, painted again once the logo and the faces are in.
	const room = staged?.room;
	const venue = useMemo(() => {
		if (!made || !room) {
			return undefined;
		}
		const { team, draftNight: night } = made;
		return draftVenue(
			paintDraftVenue(
				{
					season: made.season,
					team,
					logo: wallLogo,
					board: [
						{ overall: night.overall, label: team.region, now: true },
						...night.next.map((n) => ({ overall: n.overall, label: n.region })),
					],
					...(faces ? { faces } : {}),
				},
				room,
			),
			room,
		);
	}, [made, room, wallLogo, faces]);
	const capOn = staged?.night.capOn;
	const dressing = useMemo(() => {
		const cap = capOf(colors);
		// Bare-headed till the cap is handed him at the foot of the steps:
		// his own head and hair (kept for each look he is in, so his
		// pictures, kept by it, are).
		const bareHeads = new Map<number, HeadSprite | undefined>();
		const cuts = new Map<number, Look["cut"]>();
		const bare = new WeakMap<Look, Look>();
		return {
			dress: (who: number, look: Look): Look => {
				cuts.set(who, look.cut);
				return draftLook(who, look, cap);
			},
			head: async (
				who: number,
				face: Parameters<typeof cappedHead>[0],
				faceColors: [string, string, string] | undefined,
			) => {
				const [head, plain] = await Promise.all([
					(async () => cappedHead(face, cap, await crest))(),
					loadHead(face, faceColors),
				]);
				bareHeads.set(who, plain.sprite);
				// (Drawn: he can be seen now - a moment on, the court having
				// been handed it.)
				setTimeout(() => {
					setDressed(true);
				}, 60);
				return head;
			},
			lookAt: (who: number, t: number, look: Look): Look => {
				if (who !== pid || capOn === undefined || t >= capOn) {
					return look;
				}
				let out = bare.get(look);
				if (!out) {
					out = {
						...look,
						head: bareHeads.get(who) ?? look.head,
						cut: cuts.get(who) ?? look.cut,
					};
					delete out.cap;
					bare.set(look, out);
				}
				return out;
			},
		};
	}, [colors, crest, pid, capOn]);

	useEffect(() => {
		feed.set({ cover: !(preDone && dressed) });
	}, [feed, preDone, dressed]);
	useEffect(() => {
		feed.set({ run, t: 0, over: false });
	}, [feed, run]);

	const overlay = useMemo(() => {
		if (!made) {
			return undefined;
		}
		const { team, player } = made;
		return (
			<DraftBroadcast
				feed={feed}
				info={{
					season: made.season,
					round: made.round,
					pick: made.pick,
					team,
					player: { name: player.name, pos: player.pos },
					night: made.draftNight,
				}}
			/>
		);
	}, [made, feed]);

	const authored = useMemo((): Authored | undefined => {
		if (!staged || !made) {
			return undefined;
		}
		return {
			...staged,
			...dressing,
			...(venue ? { venue } : {}),
			overlay,
			onTick: (t: number) => {
				feed.set({ t });
			},
			run,
			onEnd: () => {
				over.current = true;
				feed.set({ over: true });
				onEndRef.current?.();
			},
		};
	}, [staged, made, dressing, venue, overlay, feed, run]);

	const boxScore = useMemo(
		() =>
			made
				? {
						// (Its own building each time, the same for the same pick.)
						gid: -100_000 - made.player.pid,
						season: made.season,
						numPeriods: 4,
						finals: false,
						playoffs: false,
						arena: made.arena,
						teams: [{ ...made.team, players: [made.player] }, undefined],
					}
				: undefined,
		[made],
	);

	if (!authored || !boxScore) {
		return null;
	}
	return (
		<div style={{ marginBottom: "-1rem" }}>
			<Court3D
				preview
				authored={authored}
				events={undefined}
				cursor={0}
				boxScore={boxScore}
				caption={null}
				paused={!playing || !dressed || !preDone}
				rate={1}
				follower={false}
				onReady={noop}
			/>
		</div>
	);
};

export default HeadlineStage;
