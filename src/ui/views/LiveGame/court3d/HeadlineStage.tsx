import { useEffect, useMemo, useRef, useState } from "react";
import type { HeadlineSceneSpec } from "../../../../common/headlineScene.ts";
import { toWorker } from "../../../util/toWorker.ts";
import type { headlineScene } from "../../../../worker/util/headlineScene.ts";
import type { Authored } from "./authored.ts";
import { capOf } from "./cap.ts";
import { cappedHead } from "./capHead.ts";
import Court3D from "./Court3D.tsx";
import { stageDraftWalk } from "./draftWalk.ts";
import { streetClothes, type Look } from "./figure.ts";
import { logoPicture } from "./logoPicture.ts";

// THE SCENE ITSELF (see HeadlineScene.tsx, which fetches this only once it
// is wanted): what it is made from asked of the worker, staged, and played
// by the court - in the building of the team it is about.

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

const ordinal = (n: number) => {
	const t = n % 100;
	const s =
		t >= 11 && t <= 13 ? "TH" : (["TH", "ST", "ND", "RD"][n % 10] ?? "TH");
	return `${n}${s}`;
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

	// The team's logo, for the front of the cap - asked for once.
	const logo = made?.team.imgURLSmall ?? made?.team.imgURL;
	const crest = useMemo(() => logoPicture(logo, 96), [logo]);

	// Over, it plays again from the top the next time it is asked to play.
	const [run, setRun] = useState(0);
	const over = useRef(false);
	const onEndRef = useRef(onEnd);
	onEndRef.current = onEnd;
	useEffect(() => {
		if (playing && over.current) {
			over.current = false;
			setRun((n) => n + 1);
		}
	}, [playing]);

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
	const staged = useMemo(
		() => (pid === undefined ? undefined : stageDraftWalk(pid)),
		[pid],
	);
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

	const authored = useMemo((): Authored | undefined => {
		if (!staged || !made) {
			return undefined;
		}
		const { team, player } = made;
		return {
			...staged,
			...dressing,
			card: {
				name: player.name,
				pos: player.pos,
				colors: team.colors,
				imgURL: team.imgURL,
				imgURLSmall: team.imgURLSmall,
				abbrev: team.abbrev,
				line: `${ordinal(made.pick)} PICK · ROUND ${made.round}${player.pos ? ` · ${player.pos}` : ""}`,
			},
			run,
			onEnd: () => {
				over.current = true;
				onEndRef.current?.();
			},
		};
	}, [staged, made, dressing, run]);

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
		<div
			style={{
				marginBottom: "-1rem",
				opacity: dressed ? 1 : 0,
				transition: "opacity 0.35s",
			}}
		>
			<Court3D
				preview
				authored={authored}
				events={undefined}
				cursor={0}
				boxScore={boxScore}
				caption={null}
				paused={!playing || !dressed}
				rate={1}
				follower={false}
				onReady={noop}
			/>
		</div>
	);
};

export default HeadlineStage;
