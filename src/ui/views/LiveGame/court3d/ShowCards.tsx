import { useEffect, useRef, type CSSProperties } from "react";
import { helpers } from "../../../util/helpers.ts";
import type { TeamTape } from "../../../../worker/views/liveGameInfo.ts";
import { lightness, shade } from "./figure.ts";

// THE MATCHUP BOARD over the building once it has opened, in the pregame
// show (see pregameShow.ts), the way the broadcasts put the two teams up
// before a tip: each team's logo, its record and where it stands, the two
// teams' numbers going in side by side - the better of each in its team's
// color - their leading scorers, and the line.

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

export type ShowLeader = {
	name: string;
	jerseyNumber?: string;
	pts: number;
	trb: number;
	ast: number;
};

export type ShowCardsInfo = {
	// [road, home], as the rest of it.
	teams: [ShowTeam | undefined, ShowTeam | undefined];
	tape?: [TeamTape | undefined, TeamTape | undefined];
	leaders?: [ShowLeader, ShowLeader];
	// Over it: a playoff game's series ("1st round · Game 3 · BOS leads
	// 2-1"), or the regular season. Under it: the spread, the over/under
	// and the season series, as the header has them.
	title: string;
	facts: string[];
};

const FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";
const PANEL = "rgba(10, 10, 14, 0.92)";
const GOLD = "#f2c14e";
const DIM = "#a9a4b4";

const inkOn = (bg: string) => (lightness(bg) > 0.6 ? "#111" : "#fff");
// A team's color on the board: its first - or, near black or near white,
// its second.
const mainOf = (t: ShowTeam | undefined) => {
	const c = t?.colors?.[0] ?? "#3a3a44";
	const l = lightness(c);
	return (l < 0.12 || l > 0.85) && t?.colors?.[1] ? t.colors[1] : c;
};
// A team's color behind a number on the dark board: its own, or - too dark
// to show there - the next of its colors that does.
const pillOf = (t: ShowTeam | undefined) =>
	[mainOf(t), ...(t?.colors ?? [])].find((c) => lightness(c) >= 0.2) ??
	"#e8e6ee";
const accentOf = (t: ShowTeam | undefined) => {
	const main = mainOf(t);
	return (main === t?.colors?.[0] ? t?.colors?.[1] : t?.colors?.[0]) ?? GOLD;
};
const shortName = (name: string) => {
	const n = name.trim();
	const i = n.indexOf(" ");
	return i < 0 ? n : `${n[0]}. ${n.slice(i + 1)}`;
};

const caps = (size: string, spacing: string): CSSProperties => ({
	fontSize: size,
	fontWeight: 800,
	letterSpacing: spacing,
	textTransform: "uppercase",
});

// A long nickname smaller, to fit its side of the board on a phone - and,
// two words and long, on two lines.
const nickStyle = (name: string): CSSProperties => {
	const words = name.split(" ");
	const two = words.length > 1 && name.length > 10;
	const len = Math.max(
		1,
		...(two ? words.map((w) => w.length) : [name.length]),
	);
	return {
		...caps(
			`min(${two ? 1.05 : 1.65}em, ${(38 / len).toFixed(2)}cqw)`,
			"0.01em",
		),
		lineHeight: two ? 1 : undefined,
		whiteSpace: two ? "normal" : "nowrap",
	};
};

const Logo = ({ t, size }: { t: ShowTeam | undefined; size: string }) => {
	const src = t?.imgURL ?? t?.imgURLSmall;
	return src ? (
		<img
			src={src}
			alt={t?.abbrev ?? ""}
			style={{
				width: size,
				height: size,
				objectFit: "contain",
				flex: "none",
				filter: "drop-shadow(0 0.1em 0.25em rgba(0,0,0,0.45))",
			}}
		/>
	) : null;
};

// One side of the top: its logo on its color, its city and name, its
// record and where it stands.
const Side = ({ t, right }: { t: ShowTeam | undefined; right: boolean }) => {
	const bg = mainOf(t);
	const nick = t?.name ?? t?.abbrev ?? "";
	return (
		<div
			style={{
				display: "flex",
				flexDirection: right ? "row-reverse" : "row",
				alignItems: "center",
				gap: "0.6em",
				padding: "0.7em 0.9em",
				textAlign: right ? "right" : "left",
				background: `linear-gradient(${right ? 225 : 135}deg, ${bg} 0%, ${shade(bg, -0.38)} 100%)`,
				color: inkOn(bg),
				borderBottom: `0.28em solid ${accentOf(t)}`,
				minWidth: 0,
			}}
		>
			<Logo t={t} size="3.6em" />
			<div style={{ minWidth: 0 }}>
				<div style={{ ...caps("0.7em", "0.16em"), opacity: 0.85 }}>
					{t?.region ?? ""}
				</div>
				<div
					style={{
						...nickStyle(nick),
						fontWeight: 900,
						margin: "0.08em 0 0.18em",
						overflow: "hidden",
						textOverflow: "ellipsis",
					}}
				>
					{nick}
				</div>
				<div
					style={{
						fontSize: "0.85em",
						fontVariantNumeric: "tabular-nums",
						whiteSpace: "nowrap",
					}}
				>
					{t?.record ? (
						<b style={{ fontSize: "1.25em", fontWeight: 900 }}>{t.record}</b>
					) : null}
					{t?.rank !== undefined && t.conf ? (
						<span
							style={{
								fontWeight: 700,
								opacity: 0.85,
								marginLeft: t.record ? "0.45em" : 0,
							}}
						>
							{helpers.ordinal(t.rank)} {t.conf}
						</span>
					) : null}
				</div>
			</div>
		</div>
	);
};

type Row = {
	label: string;
	of: (t: TeamTape) => number | undefined;
	text: (v: number, t: TeamTape) => string;
	// Fewer is better (points against).
	low?: boolean;
};
const ROWS: Row[] = [
	{ label: "Points", of: (t) => t.pts, text: (v) => v.toFixed(1) },
	{
		label: "Opp points",
		of: (t) => t.opp,
		text: (v) => v.toFixed(1),
		low: true,
	},
	{ label: "Rebounds", of: (t) => t.trb, text: (v) => v.toFixed(1) },
	{ label: "Assists", of: (t) => t.ast, text: (v) => v.toFixed(1) },
	{ label: "3PT %", of: (t) => t.tpPct, text: (v) => `${v.toFixed(1)}%` },
	{
		label: "Last 10",
		of: (t) => t.last10[0] - t.last10[1],
		text: (_v, t) => `${t.last10[0]}-${t.last10[1]}`,
	},
];

const Numbers = ({
	teams,
	tape,
}: {
	teams: ShowCardsInfo["teams"];
	tape: [TeamTape, TeamTape];
}) => (
	<div style={{ background: PANEL, padding: "0.45em 0" }}>
		{ROWS.map((row, k) => {
			const v = tape.map((t) => row.of(t)) as [
				number | undefined,
				number | undefined,
			];
			if (v[0] === undefined || v[1] === undefined) {
				return null;
			}
			// (The two shown to the decimal they're shown to, so a tie on the
			// board is a tie.)
			const shown = tape.map((t, i) => row.text(v[i]!, t));
			const better =
				shown[0] === shown[1] || v[0] === v[1]
					? -1
					: v[0] > v[1] !== !!row.low
						? 0
						: 1;
			const cell = (i: 0 | 1) => {
				const on = better === i;
				const c = pillOf(teams[i]);
				return (
					<span
						style={{
							textAlign: i === 0 ? "right" : "left",
							padding: "0 1.1em",
							fontWeight: 800,
							fontVariantNumeric: "tabular-nums",
							color: "#d9d6e0",
						}}
					>
						<span
							style={{
								display: "inline-block",
								minWidth: "2.6em",
								textAlign: "center",
								padding: "0.12em 0.45em",
								borderRadius: "0.25em",
								...(on
									? { background: c, color: inkOn(c), fontWeight: 900 }
									: {}),
							}}
						>
							{shown[i]}
						</span>
					</span>
				);
			};
			return (
				<div
					key={row.label}
					style={{
						display: "grid",
						gridTemplateColumns: "1fr 9.5em 1fr",
						alignItems: "center",
						height: "1.72em",
						borderTop: k > 0 ? "1px solid rgba(255,255,255,0.06)" : "none",
					}}
				>
					{cell(0)}
					<span
						style={{
							...caps("0.68em", "0.14em"),
							color: DIM,
							textAlign: "center",
							whiteSpace: "nowrap",
						}}
					>
						{row.label}
					</span>
					{cell(1)}
				</div>
			);
		})}
	</div>
);

const Leader = ({
	t,
	p,
	right,
}: {
	t: ShowTeam | undefined;
	p: ShowLeader;
	right: boolean;
}) => {
	const c = pillOf(t);
	return (
		<div
			style={{
				display: "flex",
				flexDirection: right ? "row-reverse" : "row",
				alignItems: "center",
				gap: "0.55em",
				padding: "0.35em 0.9em 0.6em",
				textAlign: right ? "right" : "left",
				minWidth: 0,
			}}
		>
			{p.jerseyNumber ? (
				<span
					style={{
						flex: "none",
						fontWeight: 900,
						fontSize: "0.95em",
						minWidth: "1.9em",
						textAlign: "center",
						padding: "0.3em 0.2em",
						borderRadius: "0.25em",
						background: c,
						color: inkOn(c),
						fontVariantNumeric: "tabular-nums",
					}}
				>
					{p.jerseyNumber}
				</span>
			) : null}
			<div style={{ minWidth: 0 }}>
				<div
					style={{
						...caps("0.92em", "0.02em"),
						fontWeight: 900,
						whiteSpace: "nowrap",
						overflow: "hidden",
						textOverflow: "ellipsis",
					}}
				>
					{shortName(p.name)}
				</div>
				<div
					style={{
						fontSize: "0.7em",
						fontWeight: 700,
						letterSpacing: "0.06em",
						color: DIM,
						marginTop: "0.25em",
						whiteSpace: "nowrap",
					}}
				>
					{(
						[
							[p.pts, "PPG"],
							[p.trb, "RPG"],
							[p.ast, "APG"],
						] as const
					).map(([v, label], k) => (
						<span key={label} style={{ marginLeft: k > 0 ? "0.6em" : 0 }}>
							<b style={{ color: "#fff", fontSize: "1.2em", fontWeight: 900 }}>
								{v.toFixed(1)}
							</b>{" "}
							{label}
						</span>
					))}
				</div>
			</div>
		</div>
	);
};

export const MatchupBoard = ({ info }: { info: ShowCardsInfo }) => {
	const ref = useRef<HTMLDivElement | null>(null);
	// Slid up into place as it goes up.
	useEffect(() => {
		ref.current?.animate?.(
			[
				{ opacity: 0, transform: "translateY(1.2em)" },
				{ opacity: 1, transform: "translateY(0)" },
			],
			{ duration: 360, easing: "cubic-bezier(.2,.8,.2,1)" },
		);
	}, []);
	const [road, home] = info.teams;
	const tape =
		info.tape?.[0] && info.tape[1]
			? ([info.tape[0], info.tape[1]] as [TeamTape, TeamTape])
			: undefined;
	return (
		<div
			style={{
				position: "absolute",
				inset: 0,
				pointerEvents: "none",
				// The building dimmed round it.
				background:
					"radial-gradient(ellipse at 50% 45%, rgba(4,4,8,0.3), rgba(4,4,8,0.78))",
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				fontFamily: FONT,
				fontSize: "clamp(9.5px, calc(0.9cqw + 6.5px), 16px)",
				lineHeight: 1.1,
				color: "#fff",
			}}
		>
			<div
				ref={ref}
				style={{
					width: "min(94cqw, 44em)",
					borderRadius: "0.35em",
					overflow: "hidden",
					boxShadow: "0 0.6em 2.4em rgba(0,0,0,0.7)",
				}}
			>
				<div
					style={{
						...caps("0.72em", "0.18em"),
						fontWeight: 900,
						background: "rgba(10,10,14,0.94)",
						color: GOLD,
						textAlign: "center",
						padding: "0.55em 1em 0.5em",
						whiteSpace: "nowrap",
						overflow: "hidden",
						textOverflow: "ellipsis",
					}}
				>
					{info.title}
				</div>
				<div style={{ display: "grid", gridTemplateColumns: "1fr auto 1fr" }}>
					<Side t={road} right={false} />
					<div
						style={{
							display: "flex",
							alignItems: "center",
							justifyContent: "center",
							background: "#0a0a0e",
							color: GOLD,
							fontWeight: 900,
							fontSize: "1.25em",
							padding: "0 0.55em",
						}}
					>
						@
					</div>
					<Side t={home} right />
				</div>
				{tape ? <Numbers teams={info.teams} tape={tape} /> : null}
				{info.leaders ? (
					<div
						style={{
							display: "grid",
							gridTemplateColumns: "1fr 1fr",
							background: "rgba(18,18,24,0.95)",
							borderTop: "1px solid rgba(255,255,255,0.08)",
						}}
					>
						<div
							style={{
								...caps("0.62em", "0.2em"),
								fontWeight: 900,
								gridColumn: "1 / -1",
								textAlign: "center",
								color: GOLD,
								padding: "0.6em 0 0.1em",
							}}
						>
							Leading scorers
						</div>
						<Leader t={road} p={info.leaders[0]} right={false} />
						<Leader t={home} p={info.leaders[1]} right />
					</div>
				) : null}
				{info.facts.length > 0 ? (
					<div
						style={{
							...caps("0.72em", "0.12em"),
							background: "#0a0a0e",
							color: "#d9d6e0",
							textAlign: "center",
							padding: "0.5em 1em",
							fontVariantNumeric: "tabular-nums",
							whiteSpace: "nowrap",
							overflow: "hidden",
							textOverflow: "ellipsis",
						}}
					>
						{info.facts.join(" · ")}
					</div>
				) : null}
			</div>
		</div>
	);
};
