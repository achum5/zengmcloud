import { useEffect, useRef, useSyncExternalStore } from "react";
import { helpers } from "../../../util/helpers.ts";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import type { DraftFeed } from "./DraftBroadcast.tsx";
import { lightness } from "./figure.ts";

// THE PRESS CONFERENCE, AS IT IS BROADCAST (see pressStage.ts,
// PressStage.tsx): the team's colors and logo over everything while the room
// is got ready; then a bar along the bottom a few seconds at a time - the
// team, his name (theirs, if it got several), and how he came: who he was
// got from, or what he signed for.
//
// Graphics only: nothing here touches the league.

const FONT = '"Recap Condensed", "Arial Narrow", system-ui, sans-serif';

type TeamInfo = {
	region: string;
	name: string;
	colors: [string, string, string];
	imgURL?: string;
	imgURLSmall?: string;
};

export type PressInfo = {
	team: TeamInfo;
	players: { name: string; pos?: string; age: number }[];
	kind: "trade" | "signing";
	from?: { region: string; name: string };
	contract?: { amount: number; years: number };
	// When the bar is up (ms of the scene).
	bars: [number, number][];
};

const inkOn = (c: string) => (lightness(c) > 0.62 ? "#111" : "#fff");

const useEnter = (key: string, from: string) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		ref.current?.animate?.(
			[
				{ opacity: 0, transform: from },
				{ opacity: 1, transform: "none" },
			],
			{ duration: 340, easing: "cubic-bezier(.2,.8,.2,1)" },
		);
	}, [key, from]);
	return ref;
};

// The team's colors and logo, over the room while it is got ready.
const Cover = ({ info, shown }: { info: PressInfo; shown: boolean }) => {
	const [main, accent] = info.team.colors;
	return (
		<div
			style={{
				position: "absolute",
				inset: 0,
				zIndex: 3,
				overflow: "hidden",
				background: `linear-gradient(120deg, ${main} 0%, ${main} 62%, ${accent} 62%, ${accent} 64%, ${main} 64%)`,
				opacity: shown ? 1 : 0,
				transition: "opacity 420ms",
				pointerEvents: "none",
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
			}}
		>
			<TeamLogoInline
				alt=""
				imgURL={info.team.imgURL}
				imgURLSmall={info.team.imgURL ?? info.team.imgURLSmall}
				style={{ width: "34cqw", height: "34cqw" }}
			/>
		</div>
	);
};

// What it says under his name: how he came, and what he is.
const lineOf = (info: PressInfo) => {
	const how =
		info.kind === "trade" && info.from
			? `Acquired from ${info.from.region} ${info.from.name}`
			: info.contract
				? `Signed · ${info.contract.years} yr${info.contract.years === 1 ? "" : "s"} · ${helpers.formatCurrency((info.contract.amount * info.contract.years) / 1000, "M")}`
				: info.kind === "trade"
					? "Acquired by trade"
					: "Signed as a free agent";
	const one = info.players.length === 1 ? info.players[0] : undefined;
	return one?.pos ? `${one.pos} · Age ${one.age} · ${how}` : how;
};

const Bar = ({ info }: { info: PressInfo }) => {
	const [main, accent] = info.team.colors;
	const ink = inkOn(main);
	const barRef = useEnter("bar", "translateX(-6%)");
	const names =
		info.players.length === 1
			? info.players[0]!.name
			: info.players.map((p) => p.name.split(" ").at(-1)).join(" · ");
	return (
		<div
			ref={barRef}
			style={{
				position: "absolute",
				left: "4cqw",
				right: "4cqw",
				bottom: "5cqw",
				zIndex: 2,
				fontFamily: FONT,
				fontSize: "clamp(12px, 2.3cqw, 22px)",
				lineHeight: 1.05,
				whiteSpace: "nowrap",
				pointerEvents: "none",
				display: "flex",
				alignItems: "stretch",
				filter: "drop-shadow(0 3px 10px rgba(0,0,0,.5))",
			}}
		>
			<div
				style={{
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					padding: "0.25em",
					background: "#fff",
					width: "2.9em",
					flexShrink: 0,
				}}
			>
				<TeamLogoInline
					alt=""
					imgURL={info.team.imgURL}
					imgURLSmall={info.team.imgURLSmall ?? info.team.imgURL}
					style={{ width: "2.4em", height: "2.4em" }}
				/>
			</div>
			<div style={{ minWidth: 0, display: "flex", flexDirection: "column" }}>
				<div
					style={{
						background: main,
						color: ink,
						padding: "0.22em 0.6em 0.12em",
						fontSize: "1.55em",
						fontWeight: 700,
						textTransform: "uppercase",
						letterSpacing: "0.02em",
						overflow: "hidden",
						textOverflow: "ellipsis",
						borderTop: `0.14em solid ${accent}`,
					}}
				>
					{names}
				</div>
				<div
					style={{
						background: "#0d0e12",
						color: "#f2f2f2",
						padding: "0.2em 0.75em",
						fontSize: "0.95em",
						fontWeight: 600,
						letterSpacing: "0.04em",
						overflow: "hidden",
						textOverflow: "ellipsis",
						alignSelf: "flex-start",
						maxWidth: "100%",
					}}
				>
					{lineOf(info)}
				</div>
			</div>
		</div>
	);
};

export const PressBroadcast = ({
	info,
	feed,
}: {
	info: PressInfo;
	feed: DraftFeed;
}) => {
	const shown = useSyncExternalStore(feed.subscribe, () => {
		const s = feed.get();
		const bar = info.bars.findIndex(([a, b]) => s.t >= a && s.t < b);
		return `${s.cover ? 1 : 0}|${s.run}|${bar}`;
	});
	const [cover, run, bar] = shown.split("|") as [string, string, string];
	return (
		<>
			{cover === "0" && bar !== "-1" ? (
				<Bar key={`bar${run}|${bar}`} info={info} />
			) : null}
			<Cover key={`cover${run}`} info={info} shown={cover === "1"} />
		</>
	);
};
