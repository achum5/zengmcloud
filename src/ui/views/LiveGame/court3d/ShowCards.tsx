import { useEffect, useRef, type ReactNode } from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { helpers } from "../../../util/helpers.ts";
import { lightness } from "./figure.ts";
import type { ShowCardKind } from "./pregameShow.ts";

// THE BROADCAST'S CARDS over the two lines at the end of the pregame show
// (see pregameShow.ts), one after another: the matchup - the two teams,
// their records, where they stand - then the two starting fives, and the
// key matchup, each team's leading scorer and his numbers going in.

export type ShowTeam = {
	abbrev?: string;
	region?: string;
	name?: string;
	colors?: [string, string, string];
	imgURL?: string;
	imgURLSmall?: string;
	record?: string;
	rank?: number;
	conf?: string;
};

export type ShowStarter = {
	pid: number;
	name: string;
	jerseyNumber?: string;
	pos?: string;
};

export type ShowLeader = {
	name: string;
	jerseyNumber?: string;
	pos?: string;
	pts: number;
	trb: number;
	ast: number;
};

export type ShowCardsInfo = {
	// [road, home].
	teams: [ShowTeam | undefined, ShowTeam | undefined];
	starters: [ShowStarter[], ShowStarter[]];
	leaders?: [ShowLeader, ShowLeader];
	// A playoff game's series ("Game 3 · BOS leads 2-1").
	series?: string;
};

const FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";
const PANEL = "rgba(10, 10, 14, 0.92)";
const GOLD = "#f2c14e";

const inkOn = (bg: string) => (lightness(bg) > 0.62 ? "#111" : "#fff");
const mainOf = (t: ShowTeam | undefined) => t?.colors?.[0] ?? "#3a3a44";
const accentOf = (t: ShowTeam | undefined) => t?.colors?.[1] ?? "#9a9aa6";
const lastName = (name: string) => {
	const n = name.trim();
	const i = n.indexOf(" ");
	return i < 0 ? n : n.slice(i + 1);
};

// Slid up into place each time a new card goes up.
const Card = ({ k, children }: { k: number; children: ReactNode }) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		ref.current?.animate?.(
			[
				{ opacity: 0, transform: "translate(-50%, 1.2em)" },
				{ opacity: 1, transform: "translate(-50%, 0)" },
			],
			{ duration: 320, easing: "cubic-bezier(.2,.8,.2,1)" },
		);
	}, [k]);
	return (
		<div
			ref={ref}
			style={{
				position: "absolute",
				left: "50%",
				bottom: "5cqw",
				transform: "translate(-50%, 0)",
				fontFamily: FONT,
				fontSize: "clamp(8px, 1.75cqw, 18px)",
				lineHeight: 1.1,
				color: "#fff",
				borderRadius: 4,
				overflow: "hidden",
				boxShadow: "0 4px 16px rgba(0,0,0,.65)",
				whiteSpace: "nowrap",
				pointerEvents: "none",
			}}
		>
			{children}
		</div>
	);
};

const Logo = ({ t, size }: { t: ShowTeam | undefined; size: string }) => (
	<TeamLogoInline
		alt={t?.abbrev}
		imgURL={t?.imgURL}
		imgURLSmall={t?.imgURLSmall}
		style={{ height: size, width: size }}
	/>
);

const Header = ({ text }: { text: string }) => (
	<div
		style={{
			background: PANEL,
			color: GOLD,
			fontWeight: 900,
			letterSpacing: "0.16em",
			fontSize: "0.82em",
			textAlign: "center",
			padding: "0.4em 1em",
		}}
	>
		{text}
	</div>
);

// One side of the matchup: its logo on its color, its letters, its record.
const MatchupSide = ({
	t,
	right,
}: {
	t: ShowTeam | undefined;
	right: boolean;
}) => {
	const bg = mainOf(t);
	const ink = inkOn(bg);
	return (
		<div
			style={{
				display: "flex",
				flexDirection: right ? "row-reverse" : "row",
				alignItems: "center",
				gap: "0.6em",
				padding: "0.55em 0.9em",
				background: bg,
				color: ink,
				borderBottom: `0.2em solid ${accentOf(t)}`,
				minWidth: "9em",
			}}
		>
			<Logo t={t} size="2.6em" />
			<div
				style={{
					display: "flex",
					flexDirection: "column",
					alignItems: right ? "flex-end" : "flex-start",
					gap: "0.2em",
				}}
			>
				<span
					style={{ fontSize: "2em", fontWeight: 900, letterSpacing: "0.04em" }}
				>
					{t?.abbrev ?? ""}
				</span>
				<span
					style={{
						fontSize: "0.8em",
						fontWeight: 700,
						letterSpacing: "0.08em",
						opacity: 0.9,
						fontVariantNumeric: "tabular-nums",
					}}
				>
					{[
						t?.record,
						t?.rank !== undefined && t.conf
							? `${helpers.ordinal(t.rank)} ${t.conf}`
							: undefined,
					]
						.filter(Boolean)
						.join(" · ")}
				</span>
			</div>
		</div>
	);
};

const Matchup = ({ info }: { info: ShowCardsInfo }) => {
	const [road, home] = info.teams;
	return (
		<div style={{ display: "flex", flexDirection: "column" }}>
			<div style={{ display: "flex", alignItems: "stretch" }}>
				<MatchupSide t={road} right={false} />
				<div
					style={{
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
						background: PANEL,
						color: GOLD,
						fontWeight: 900,
						fontSize: "1.3em",
						padding: "0 0.7em",
					}}
				>
					@
				</div>
				<MatchupSide t={home} right />
			</div>
			{info.series ? (
				<div
					style={{
						background: PANEL,
						color: "#e6e2ec",
						fontWeight: 700,
						fontSize: "0.78em",
						letterSpacing: "0.08em",
						textAlign: "center",
						padding: "0.35em 1em",
					}}
				>
					{info.series}
				</div>
			) : null}
		</div>
	);
};

const FiveOf = ({
	t,
	five,
}: {
	t: ShowTeam | undefined;
	five: ShowStarter[];
}) => (
	<div style={{ display: "flex", alignItems: "stretch" }}>
		<div
			style={{
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				padding: "0 0.55em",
				background: mainOf(t),
				borderRight: `0.18em solid ${accentOf(t)}`,
			}}
		>
			<Logo t={t} size="2.3em" />
		</div>
		<div
			style={{
				display: "grid",
				gridTemplateColumns: "auto auto auto",
				columnGap: "0.6em",
				rowGap: "0.18em",
				padding: "0.45em 0.8em",
				background: PANEL,
				alignContent: "center",
			}}
		>
			{five.map((p) => (
				<div key={p.pid} style={{ display: "contents" }}>
					<span
						style={{
							color: GOLD,
							fontWeight: 800,
							textAlign: "right",
							fontVariantNumeric: "tabular-nums",
						}}
					>
						{p.jerseyNumber ?? ""}
					</span>
					<span
						style={{
							fontWeight: 800,
							textTransform: "uppercase",
							letterSpacing: "0.03em",
						}}
					>
						{lastName(p.name)}
					</span>
					<span style={{ color: "#bdb8c6", fontWeight: 700 }}>
						{p.pos ?? ""}
					</span>
				</div>
			))}
		</div>
	</div>
);

const Lineups = ({ info }: { info: ShowCardsInfo }) => (
	<div style={{ display: "flex", flexDirection: "column" }}>
		<Header text="STARTING LINEUPS" />
		<div style={{ display: "flex", gap: "0.15em", background: PANEL }}>
			<FiveOf t={info.teams[0]} five={info.starters[0]} />
			<FiveOf t={info.teams[1]} five={info.starters[1]} />
		</div>
	</div>
);

const LeaderSide = ({
	t,
	p,
	right,
}: {
	t: ShowTeam | undefined;
	p: ShowLeader;
	right: boolean;
}) => (
	<div
		style={{
			display: "flex",
			flexDirection: right ? "row-reverse" : "row",
			alignItems: "stretch",
		}}
	>
		<div
			style={{
				display: "flex",
				alignItems: "center",
				padding: "0 0.55em",
				background: mainOf(t),
			}}
		>
			<Logo t={t} size="2.3em" />
		</div>
		<div
			style={{
				display: "flex",
				flexDirection: "column",
				alignItems: right ? "flex-end" : "flex-start",
				gap: "0.3em",
				padding: "0.45em 0.8em",
				background: PANEL,
				borderBottom: `0.18em solid ${accentOf(t)}`,
			}}
		>
			<span
				style={{
					fontWeight: 900,
					textTransform: "uppercase",
					letterSpacing: "0.03em",
					fontSize: "1.05em",
				}}
			>
				{p.name}
			</span>
			<div
				style={{
					display: "flex",
					gap: "0.9em",
					fontVariantNumeric: "tabular-nums",
				}}
			>
				{(
					[
						[p.pts, "PPG"],
						[p.trb, "RPG"],
						[p.ast, "APG"],
					] as const
				).map(([v, label]) => (
					<span
						key={label}
						style={{
							display: "flex",
							flexDirection: "column",
							alignItems: "center",
						}}
					>
						<span style={{ fontWeight: 900, fontSize: "1.15em" }}>
							{v.toFixed(1)}
						</span>
						<span
							style={{
								fontSize: "0.62em",
								fontWeight: 700,
								color: "#bdb8c6",
								letterSpacing: "0.1em",
							}}
						>
							{label}
						</span>
					</span>
				))}
			</div>
		</div>
	</div>
);

const KeyMatchup = ({
	info,
	leaders,
}: {
	info: ShowCardsInfo;
	leaders: [ShowLeader, ShowLeader];
}) => (
	<div style={{ display: "flex", flexDirection: "column" }}>
		<Header text="KEY MATCHUP" />
		<div style={{ display: "flex", gap: "0.15em", background: PANEL }}>
			<LeaderSide t={info.teams[0]} p={leaders[0]} right={false} />
			<LeaderSide t={info.teams[1]} p={leaders[1]} right />
		</div>
	</div>
);

export const ShowCards = ({
	card,
	info,
}: {
	card: { kind: ShowCardKind; k: number } | undefined;
	info: ShowCardsInfo;
}) => {
	if (!card) {
		return null;
	}
	if (card.kind === "keyMatchup") {
		return info.leaders ? (
			<Card k={card.k}>
				<KeyMatchup info={info} leaders={info.leaders} />
			</Card>
		) : null;
	}
	return (
		<Card k={card.k}>
			{card.kind === "matchup" ? (
				<Matchup info={info} />
			) : (
				<Lineups info={info} />
			)}
		</Card>
	);
};
