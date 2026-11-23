import { useEffect, useMemo, useRef, useState } from "react";
import type { HeadlineSceneSpec } from "../../../../common/headlineScene.ts";
import { toWorker } from "../../../util/toWorker.ts";
import type { headlineScene } from "../../../../worker/util/headlineScene.ts";
import type { Authored } from "./authored.ts";
import { capOf } from "./cap.ts";
import { cappedHead } from "./capHead.ts";
import Court3D from "./Court3D.tsx";
import {
	DraftBroadcast,
	makeDraftFeed,
	PRE_ROLL_MS,
} from "./DraftBroadcast.tsx";
import { crowdFaces, type CrowdFace } from "./crowdFaces.ts";
import { stageDraftNight } from "./draftStage.ts";
import { draftVenue, paintDraftVenue } from "./draftVenue.ts";
import { streetClothes, type Look } from "./figure.ts";
import { logoPicture } from "./logoPicture.ts";

// THE SCENE ITSELF (see HeadlineScene.tsx, which fetches this only once it
// is wanted): what it is made from asked of the worker, staged, and played
// by the court - in the building of the team it is about, dressed for the
// draft (see draftVenue.ts).

type Made = NonNullable<Awaited<ReturnType<typeof headlineScene>>>;

const noop = () => {};

// A draft pick's suit: dark, a white shirt, a tie in his new team's color.
const SUITS = ["#1d2333", "#24262c", "#151517", "#2c3140"];
const draftSuit = (
	pid: number,
	tie: string,
): Pick<Look, "kit" | "gear" | "outfit"> => {
	const worn = streetClothes(pid);
	const suit = SUITS[Math.abs(pid) % SUITS.length]!;
	const shoe = "#141416";
	const sole = "#2a2a2c";
	return {
		kit: {
			...worn.kit,
			jersey: suit,
			trim: suit,
			number: suit,
			numberEdge: suit,
			shorts: suit,
			stripe: suit,
			sock: suit,
			shoe,
			sole,
		},
		gear: { tights: { legs: "RL", color: suit }, shoe, sole, sock: suit },
		outfit: { sleeves: "long", shirt: "#f2f2ee", tie },
	};
};

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
		void (async () => {
			try {
				const got = await toWorker("main", "getHeadlineScene", {
					type,
					pid: scenePid,
				});
				if (!alive) {
					return;
				}
				if (got) {
					setMade(got);
				} else {
					onNoneRef.current();
				}
			} catch {
				if (alive) {
					onNoneRef.current();
				}
			}
		})();
		return () => {
			alive = false;
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
		() => (pid === undefined ? undefined : stageDraftNight(pid, colors, race)),
		[pid, colors, race],
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
	const dressing = useMemo(() => {
		const cap = capOf(colors);
		return {
			dress: (who: number, look: Look): Look => {
				const out: Look = {
					...look,
					...draftSuit(who, cap.crown),
					jerseyNumber: "",
					lastName: "",
					wordmark: "",
					street: true,
					cap,
					// (His hair under it: close to his head, unless it hangs down
					// past it.)
					cut:
						look.cut === "long"
							? "long"
							: look.cut === "bald"
								? "bald"
								: "short",
				};
				delete out.kitArt;
				delete out.crest;
				delete out.style;
				return out;
			},
			head: async (_pid: number, face: Parameters<typeof cappedHead>[0]) => {
				const head = await cappedHead(face, cap, await crest);
				// (Drawn: he can be seen now - a moment on, the court having
				// been handed it.)
				setTimeout(() => {
					setDressed(true);
				}, 60);
				return head;
			},
		};
	}, [colors, crest]);

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
