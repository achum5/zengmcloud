import { useEffect, useState, type ComponentType, type ReactNode } from "react";
import { toWorker } from "../../util/toWorker.ts";
import { EmbeddedView } from "../../components/EmbeddedView.tsx";
import {
	DOCK_ICONS,
	RecapSections,
	type RecapSectionDef,
} from "../LiveGame/gamecast/RecapDock.tsx";
import { TeamGames } from "./TeamGames.tsx";
import TeamFinances from "../TeamFinances/index.tsx";
import DraftPicks from "../DraftPicks.tsx";
import Transactions from "../Transactions.tsx";
import type { ViewId } from "../../router/types.ts";

// ANOTHER TEAM PAGE, DRAWN AS A SECTION: its data asked of the worker as its
// own page asks for it (once the section is drawn - when it comes near the
// window), then BBGM's own view of it, embedded (no title bar, no header of
// its own - see EmbeddedView).
const ViewPart = ({
	viewId,
	params,
	Component,
}: {
	viewId: ViewId;
	params: Record<string, string>;
	Component: ComponentType<any>;
}) => {
	const [data, setData] = useState<Record<string, unknown>>();
	const key = JSON.stringify(params);
	useEffect(() => {
		let live = true;
		(async () => {
			try {
				const got = await toWorker("main", "runBefore", {
					viewId,
					params: JSON.parse(key),
					ctxBBGM: {},
					updateEvents: new Set(["firstRun"] as const),
					prevOutput: {},
					prevInputs: {},
				});
				const d = got?.data;
				if (live && d && !d.errorMessage && !d.redirectUrl) {
					setData(d);
				}
			} catch {
				// (Nothing to show.)
			}
		})();
		return () => {
			live = false;
		};
	}, [viewId, key]);
	if (!data) {
		return <div className="hub-wait" />;
	}
	return (
		<EmbeddedView.Provider value={true}>
			<Component {...data} />
		</EmbeddedView.Provider>
	);
};

// A TEAM'S PAGE AS ONE LONG PAGE (the refreshed look, any season): its
// roster, then its games that season (each to its game's page), its finances,
// draft picks (this season) and transactions - a menu at the foot of the
// window to go between them. Each part but
// the games is BBGM's own page of it.
export const TeamHub = ({
	abbrev,
	tid,
	season,
	currentSeason,
	roster,
}: {
	abbrev: string;
	tid: number;
	season: number;
	currentSeason: number;
	roster: ReactNode;
}) => {
	// "abbrev_tid", as the team's pages take it.
	const team = `${abbrev}_${tid}`;
	const current = season === currentSeason;
	const sections: RecapSectionDef[] = [
		{
			id: "roster",
			label: "Roster",
			icon: DOCK_ICONS.player,
			title: null,
			// (Its button to the very top of the window: the team's banner over
			// the roster too.)
			toTop: true,
			body: roster,
		},
		{
			id: "schedule",
			label: "Schedule",
			icon: DOCK_ICONS.schedule,
			body: <TeamGames tid={tid} abbrev={abbrev} season={season} />,
		},
		{
			id: "finances",
			label: "Finances",
			icon: DOCK_ICONS.finances,
			body: (
				<ViewPart
					viewId="teamFinances"
					params={{ team, show: "10" }}
					Component={TeamFinances}
				/>
			),
		},
		{
			id: "picks",
			label: "Picks",
			title: "Draft Picks",
			icon: DOCK_ICONS.picks,
			body: (
				<ViewPart
					viewId="draftPicks"
					params={{ team }}
					Component={DraftPicks}
				/>
			),
		},
		{
			id: "moves",
			label: "Moves",
			title: "Transactions",
			icon: DOCK_ICONS.moves,
			body: (
				<ViewPart
					viewId="transactions"
					params={{ team, season: String(season) }}
					Component={Transactions}
				/>
			),
		},
	];
	return (
		<div className="recap team-hub">
			<RecapSections
				// (Its draft picks are this season's.)
				sections={current ? sections : sections.filter((x) => x.id !== "picks")}
			/>
		</div>
	);
};
