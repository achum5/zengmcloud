import { useEffect, useMemo, useRef, useState } from "react";
import { toWorker } from "../../../util/toWorker.ts";
import type { draftClock } from "../../../../worker/util/headlineScene.ts";
import type { Authored } from "./authored.ts";
import Court3D from "./Court3D.tsx";
import { DraftClockBroadcast, makeDraftFeed } from "./DraftBroadcast.tsx";
import { ASK_AGAIN_MS, clockShotAt, stageOnTheClock } from "./draftClock.ts";
import { draftLook } from "./draftDress.ts";
import { crowdFaces, type CrowdFace } from "./crowdFaces.ts";
import { draftVenue, paintDraftVenue } from "./draftVenue.ts";
import type { Look } from "./figure.ts";
import { logoPicture } from "./logoPicture.ts";

// THE ROOM BETWEEN PICKS ON THE DRAFT PAGE (see HeadlineScene.tsx, which
// fetches this only once it is wanted, and draftClock.ts): the team on the
// clock as the worker has it now, its colors on the wall, the cameras going
// round the room and in on the best left at their tables. Asked for again
// each time it is put up - once a pick is in, for the next.

type Made = NonNullable<Awaited<ReturnType<typeof draftClock>>>;

const noop = () => {};

const ClockStage = ({
	playing,
	onNone,
}: {
	playing: boolean;
	onNone: () => void;
}) => {
	const [made, setMade] = useState<Made>();
	const onNoneRef = useRef(onNone);
	onNoneRef.current = onNone;
	useEffect(() => {
		let alive = true;
		let timer: ReturnType<typeof setTimeout> | undefined;
		// (Asked again a few times before it is given up on: the worker can be
		// in the middle of a pick, or of a league mate's coming in.)
		const ask = async (i: number) => {
			let got: Made | undefined;
			try {
				got = await toWorker("main", "getDraftClock", undefined);
			} catch (error) {
				console.warn("Draft room", error);
			}
			if (!alive) {
				return;
			}
			if (got) {
				setMade(got);
			} else if (i + 1 < ASK_AGAIN_MS.length) {
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
	}, []);

	// The team's logo, big, for the LED wall; the faces round the tables.
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

	const staged = useMemo(
		() =>
			made
				? stageOnTheClock(
						made.prospects.map((p) => ({ pid: p.pid, race: p.race })),
					)
				: undefined,
		[made],
	);
	// Those the cameras go to, in the order they go to them.
	const seated = useMemo(
		() =>
			staged && made
				? staged.seated
						.map((pid) => made.prospects.find((p) => p.pid === pid))
						.filter((p) => p !== undefined)
				: [],
		[staged, made],
	);
	const room = staged?.room;
	const venue = useMemo(() => {
		if (!made || !room) {
			return undefined;
		}
		const { team } = made;
		return draftVenue(
			paintDraftVenue(
				{
					season: made.season,
					team,
					logo: wallLogo,
					board: [
						{ overall: made.overall, label: team.region, now: true },
						...made.next.map((n) => ({ overall: n.overall, label: n.region })),
					],
					...(faces ? { faces } : {}),
				},
				room,
			),
			room,
		);
	}, [made, room, wallLogo, faces]);

	const feed = useMemo(() => makeDraftFeed(), []);
	const overlay = useMemo(() => {
		if (!made) {
			return undefined;
		}
		const n = seated.length;
		return (
			<DraftClockBroadcast
				feed={feed}
				info={{
					season: made.season,
					round: made.round,
					pick: made.pick,
					overall: made.overall,
					team: made.team,
					next: made.next,
					best: made.best,
					prospects: seated,
				}}
				prospectAt={(t) => {
					const { shot } = clockShotAt(t, n);
					return shot.kind === "prospect" ? shot.i : undefined;
				}}
			/>
		);
	}, [made, seated, feed]);

	const authored = useMemo((): Authored | undefined => {
		if (!staged || !made) {
			return undefined;
		}
		return {
			...staged,
			// (Not taken yet: no cap.)
			dress: (who: number, look: Look): Look => draftLook(who, look),
			...(venue ? { venue } : {}),
			overlay,
			onTick: (t: number) => {
				feed.set({ t });
			},
			run: 0,
		};
	}, [staged, made, venue, overlay, feed]);

	const boxScore = useMemo(
		() =>
			made
				? {
						// (Its own building for each pick.)
						gid: -300_000 - made.overall,
						season: made.season,
						numPeriods: 4,
						finals: false,
						playoffs: false,
						arena: made.arena,
						teams: [
							{
								...made.team,
								players: seated.map((p) => ({
									pid: p.pid,
									name: p.name,
									pos: p.pos,
									jerseyNumber: "",
								})),
							},
							undefined,
						],
					}
				: undefined,
		[made, seated],
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
				paused={!playing}
				rate={1}
				follower={false}
				onReady={noop}
			/>
		</div>
	);
};

export default ClockStage;
