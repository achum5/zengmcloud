import { useMemo } from "react";
import type { ArenaLooks, CourtStyle } from "../../../../common/types.ts";
import Court3D from "./Court3D.tsx";

// A COURT WITH NO GAME ON IT: the home floor as the 3D game lays it, in its
// building, the two teams out warming up on it - what the court editor shows
// of the court being made.

export type PreviewTeam = {
	tid: number;
	abbrev: string;
	region: string;
	name: string;
	colors: [string, string, string];
	jersey?: string;
	imgURL?: string;
	imgURLSmall?: string;
	jerseySkins?: unknown;
	players: {
		pid: number;
		name: string;
		pos?: string;
		skills?: string[];
		jerseyNumber?: string;
	}[];
};

const noop = () => {};

const CourtPreview = ({
	home,
	away,
	court,
	season,
	arena,
	finals,
}: {
	home: PreviewTeam;
	away: PreviewTeam | undefined;
	// The court being made, its pictures filled in.
	court: CourtStyle;
	season: number;
	arena: ArenaLooks | undefined;
	finals: boolean;
}) => {
	const boxScore = useMemo(
		() => ({
			// (The same warm-up every time, for this team.)
			gid: -1 - home.tid,
			season,
			numPeriods: 4,
			finals,
			arena,
			teams: [{ ...home, court }, away],
		}),
		[home, away, court, season, arena, finals],
	);
	return (
		<Court3D
			preview
			events={undefined}
			cursor={0}
			boxScore={boxScore}
			caption={null}
			paused={false}
			rate={1}
			follower={false}
			onReady={noop}
		/>
	);
};

export default CourtPreview;
