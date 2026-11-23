import { useEffect, useMemo, useRef, useState } from "react";
import type { HeadlineSceneSpec } from "../../../../common/headlineScene.ts";
import { toWorker } from "../../../util/toWorker.ts";
import type { pressConference } from "../../../../worker/util/pressConference.ts";
import type { Authored } from "./authored.ts";
import Court3D from "./Court3D.tsx";
import { makeDraftFeed } from "./DraftBroadcast.tsx";
import { ASK_AGAIN_MS } from "./draftClock.ts";
import { loadHead } from "./faces.ts";
import type { Look } from "./figure.ts";
import { logoPicture } from "./logoPicture.ts";
import { PressBroadcast, type PressInfo } from "./PressBroadcast.tsx";
import { pressLook } from "./pressDress.ts";
import { stagePress } from "./pressStage.ts";
import { paintPressVenue, pressVenue } from "./pressVenue.ts";

// A TEAM'S PRESS CONFERENCE FOR WHO IT GOT (see HeadlineScene.tsx, which
// fetches this only once it is wanted; pressStage.ts for the scene): what it
// is made from asked of the worker, staged, and played by the court - in a
// press room of the team's own (see pressVenue.ts).

type Made = NonNullable<Awaited<ReturnType<typeof pressConference>>>;

const noop = () => {};
// How long the team's colors are up before the room is shown (ms).
const PRE_ROLL_MS = 1100;

const PressStage = ({
	scene,
	playing,
	onEnd,
	onNone,
}: {
	scene: Extract<HeadlineSceneSpec, { type: "pressConference" }>;
	playing: boolean;
	onEnd?: () => void;
	onNone: () => void;
}) => {
	const { eid, tid } = scene;
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
					(await toWorker("main", "getPressConference", { eid, tid })) ??
					undefined;
			} catch (error) {
				failed = true;
				console.warn("Press conference", error);
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
	}, [eid, tid]);

	// The team's logo, big, for the backdrop.
	const bigLogo = made?.team.imgURL ?? made?.team.imgURLSmall;
	const [logo, setLogo] = useState<HTMLCanvasElement>();
	useEffect(() => {
		let alive = true;
		void logoPicture(bigLogo, 240).then((c) => {
			if (alive) {
				setLogo(c);
			}
		});
		return () => {
			alive = false;
		};
	}, [bigLogo]);

	// Over, it plays again from the top the next time it is asked to play.
	const [run, setRun] = useState(0);
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

	const staged = useMemo(
		() =>
			made
				? stagePress({
						eid: made.eid,
						season: made.season,
						team: { tid: made.team.tid, colors: made.team.colors },
						players: made.players,
					})
				: undefined,
		[made],
	);

	// Not seen till their heads are drawn (or till it is plain that they
	// never will be).
	const [dressed, setDressed] = useState(false);
	useEffect(() => {
		if (!made) {
			return;
		}
		const timer = setTimeout(() => {
			setDressed(true);
		}, 6000);
		return () => {
			clearTimeout(timer);
		};
	}, [made]);
	// Their skin, for their fists round their jerseys (see pressVenue.ts).
	const skins = useRef(new Map<number, string>());
	const dressing = useMemo(() => {
		if (!made) {
			return undefined;
		}
		const want = new Set(made.players.slice(0, 4).map((p) => p.pid));
		const done = new Set<number>();
		let cast = false;
		const check = () => {
			if (cast && [...want].every((p) => done.has(p))) {
				setTimeout(() => {
					setDressed(true);
				}, 60);
			}
		};
		return {
			onCast: () => {
				cast = true;
				check();
			},
			dress: (who: number, look: Look): Look => {
				skins.current.set(who, look.skin);
				return pressLook(who, made.eid, look);
			},
			head: async (
				who: number,
				face: Parameters<typeof loadHead>[0],
				faceColors: [string, string, string] | undefined,
			) => {
				const head = await loadHead(face, faceColors);
				if (head.skin) {
					skins.current.set(who, head.skin);
				}
				done.add(who);
				check();
				return head;
			},
		};
	}, [made]);

	const venue = useMemo(() => {
		if (!made || !staged) {
			return undefined;
		}
		return pressVenue(
			paintPressVenue({
				team: made.team,
				logo,
				take: staged.take,
				jerseys: made.players.map((p) => ({
					pid: p.pid,
					lastName: p.lastName,
					number: p.jerseyNumber,
				})),
				seats: staged.seats,
			}),
			staged.seats,
			staged.seen,
			(pid) => skins.current.get(pid),
		);
	}, [made, staged, logo]);

	useEffect(() => {
		feed.set({ cover: !(preDone && dressed) });
	}, [feed, preDone, dressed]);
	useEffect(() => {
		feed.set({ run, t: 0, over: false });
	}, [feed, run]);

	const overlay = useMemo(() => {
		if (!made || !staged) {
			return undefined;
		}
		const P = staged.plan;
		const first = P.talk.t0 === 0 ? P.talk : P.photo;
		const second = P.talk.t0 === 0 ? P.photo : P.talk;
		const info: PressInfo = {
			team: made.team,
			players: made.players.slice(0, 4).map((p) => ({
				name: p.name,
				pos: p.pos,
				age: p.age,
			})),
			kind: made.kind,
			...(made.from ? { from: made.from } : {}),
			...(made.contract ? { contract: made.contract } : {}),
			bars: [
				[700, Math.min(first.t1 - 400, 5600)],
				[second.t0 + 900, Math.min(second.t1 - 600, second.t0 + 4800)],
			],
		};
		return <PressBroadcast feed={feed} info={info} />;
	}, [made, staged, feed]);

	const authored = useMemo((): Authored | undefined => {
		if (!staged || !dressing) {
			return undefined;
		}
		return {
			show: staged.show,
			ms: staged.ms,
			moment: staged.moment,
			aim: staged.aim,
			crew: staged.crew,
			flashes: staged.flashes,
			cast: staged.cast,
			castAt: staged.castAt,
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
	}, [staged, dressing, venue, overlay, feed, run]);

	const boxScore = useMemo(
		() =>
			made
				? {
						// (Its own building each time, the same for the same one.)
						gid: -(1_000_000 + made.eid * 100 + made.team.tid),
						season: made.season,
						numPeriods: 4,
						finals: false,
						playoffs: false,
						arena: made.arena,
						teams: [
							{
								...made.team,
								players: made.players.slice(0, 4).map((p) => ({
									pid: p.pid,
									name: p.name,
									pos: p.pos,
									jerseyNumber: p.jerseyNumber,
								})),
							},
							undefined,
						],
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

export default PressStage;
