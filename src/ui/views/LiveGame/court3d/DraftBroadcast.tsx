import { useEffect, useRef, useSyncExternalStore } from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { getHeightString } from "../../../components/Height.tsx";
import { useLocal } from "../../../util/local.ts";
import { announcement, draftFacts } from "../../../../common/draftNight.ts";
import { lightness } from "./figure.ts";

// DRAFT NIGHT, AS IT IS BROADCAST, over a pick's walk-out (see draftWalk.ts,
// HeadlineStage.tsx): first the podium - the team's colors filling the
// screen, ROUND AND PICK, SELECTION, the commissioner's words and then his
// name - and then, on the way to center court, the broadcast's bar along the
// bottom: the pick and the team, his name and school, a line under it that
// goes through his age and size and the true things about him, and under that
// the draft order and the best still on the board. Once he is there, the
// next team on the clock.
//
// Graphics only: nothing here touches the draft.

const FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";
const GOLD = "#f2c14e";

// How long the podium is up before the walk-out starts (ms).
export const PRE_ROLL_MS = 5200;
// When the line under his name first comes up, and how long each stays (ms
// of the walk-out).
const LINE_FROM = 600;
const LINE_EVERY = 3000;
// How long each of the two tickers runs before the other (ms of the walk-out).
const TICKER_EVERY = 4500;

type TeamInfo = {
	region: string;
	name: string;
	abbrev: string;
	colors: [string, string, string];
	imgURL?: string;
	imgURLSmall?: string;
};

export type DraftNightInfo = {
	season: number;
	round: number;
	pick: number;
	team: TeamInfo;
	player: { name: string; pos?: string };
	night: {
		overall: number;
		rank?: number;
		best: { rank: number; name: string; pos?: string; college?: string }[];
		next: (TeamInfo & { overall: number })[];
		via?: string;
		age: number;
		hgt: number;
		weight: number;
		college?: string;
		born?: string;
		relatives: { type: string; name: string }[];
	};
};

// Where the scene is, for the graphics to keep time with it: the podium up
// or not, which run of it this is, how far into the walk-out, and whether it
// is over. Set by the stage (every frame, for t); read here only as far as it
// changes what is shown.
type FeedState = { cover: boolean; run: number; t: number; over: boolean };
export type DraftFeed = {
	get: () => FeedState;
	set: (patch: Partial<FeedState>) => void;
	subscribe: (f: () => void) => () => void;
};
export const makeDraftFeed = (): DraftFeed => {
	let state: FeedState = { cover: true, run: 0, t: 0, over: false };
	const subs = new Set<() => void>();
	return {
		get: () => state,
		set: (patch) => {
			state = { ...state, ...patch };
			for (const f of subs) {
				f();
			}
		},
		subscribe: (f) => {
			subs.add(f);
			return () => {
				subs.delete(f);
			};
		},
	};
};

const inkOn = (c: string) => (lightness(c) > 0.62 ? "#111" : "#fff");

// His school, or where he is from if he has none.
const schoolOf = (info: DraftNightInfo) =>
	info.night.college ?? info.night.born?.split(",").pop()?.trim() ?? undefined;

// A part of the picture that slides in once it is there, and again each time
// what it says changes.
const useEnter = (key: string, from: string) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		ref.current?.animate?.(
			[
				{ opacity: 0, transform: from },
				{ opacity: 1, transform: "none" },
			],
			{ duration: 320, easing: "cubic-bezier(.2,.8,.2,1)" },
		);
	}, [key, from]);
	return ref;
};

// THE PODIUM: the team's colors over everything, and the pick read out.
const Podium = ({ info, shown }: { info: DraftNightInfo; shown: boolean }) => {
	const [main, accent] = info.team.colors;
	const ink = inkOn(main);
	const school = schoolOf(info);
	const step = (ms: number) => ({
		opacity: 0,
		animation: `dn-in 420ms cubic-bezier(.2,.8,.2,1) ${ms}ms forwards`,
	});
	return (
		<div
			style={{
				position: "absolute",
				inset: 0,
				zIndex: 3,
				overflow: "hidden",
				background: `linear-gradient(115deg, ${main} 0%, ${main} 58%, ${accent} 58%, ${accent} 61%, ${main} 61%)`,
				color: ink,
				fontFamily: FONT,
				fontSize: "clamp(12px, 2.2cqw, 22px)",
				lineHeight: 1.05,
				opacity: shown ? 1 : 0,
				transition: "opacity 380ms",
				pointerEvents: "none",
			}}
		>
			<style>
				{
					"@keyframes dn-in{from{opacity:0;transform:translateX(-1.2em)}to{opacity:1;transform:none}}@keyframes dn-name{from{opacity:0;transform:scale(1.25)}to{opacity:1;transform:none}}"
				}
			</style>
			<TeamLogoInline
				alt=""
				imgURL={info.team.imgURL}
				imgURLSmall={info.team.imgURL ?? info.team.imgURLSmall}
				style={{
					position: "absolute",
					right: "-4cqw",
					top: "50%",
					width: "52cqw",
					height: "52cqw",
					transform: "translateY(-50%)",
					opacity: 0.22,
				}}
			/>
			<div
				style={{
					position: "absolute",
					left: "4cqw",
					top: "4cqw",
					padding: "0.3em 0.6em",
					background: "#fff",
					color: "#111",
					fontWeight: 900,
					fontSize: "0.75em",
					letterSpacing: "0.12em",
					borderRadius: 3,
				}}
			>
				THE PICK IS IN
			</div>
			<div
				style={{
					position: "absolute",
					left: "6cqw",
					right: "6cqw",
					top: "50%",
					transform: "translateY(-50%)",
					display: "flex",
					flexDirection: "column",
					alignItems: "flex-start",
					gap: "0.45em",
				}}
			>
				<div
					style={{
						...step(150),
						fontSize: "0.8em",
						fontWeight: 800,
						letterSpacing: "0.16em",
						color: ink === "#fff" ? GOLD : "#111",
					}}
				>
					ROUND {info.round} · PICK {info.pick}
				</div>
				<div style={step(300)}>
					<div
						style={{
							fontSize: "0.95em",
							fontWeight: 700,
							letterSpacing: "0.06em",
							textTransform: "uppercase",
						}}
					>
						{info.team.region}
					</div>
					<div
						style={{
							fontSize: "2.3em",
							fontWeight: 900,
							fontStyle: "italic",
							textTransform: "uppercase",
						}}
					>
						{info.team.name}
					</div>
				</div>
				<div
					style={{
						...step(750),
						padding: "0.3em 1.1em 0.3em 0.7em",
						background: "#0b0b10",
						color: "#fff",
						borderLeft: `0.3em solid ${accent}`,
						fontWeight: 900,
						fontStyle: "italic",
						fontSize: "1.25em",
						letterSpacing: "0.22em",
					}}
				>
					SELECTION
				</div>
				<div
					style={{
						...step(1500),
						maxWidth: "34em",
						fontSize: "0.85em",
						fontWeight: 600,
						lineHeight: 1.3,
					}}
				>
					{announcement({
						overall: info.night.overall,
						season: info.season,
						team: `${info.team.region} ${info.team.name}`,
					})}
					…
				</div>
				<div
					style={{
						opacity: 0,
						transformOrigin: "left center",
						animation: "dn-name 480ms cubic-bezier(.2,.8,.2,1) 3000ms forwards",
					}}
				>
					<div
						style={{
							fontSize: "2.6em",
							fontWeight: 900,
							textTransform: "uppercase",
							letterSpacing: "0.01em",
						}}
					>
						{info.player.name}
					</div>
					<div
						style={{
							marginTop: "0.25em",
							fontSize: "0.85em",
							fontWeight: 800,
							letterSpacing: "0.12em",
							textTransform: "uppercase",
						}}
					>
						{[info.player.pos, school].filter(Boolean).join(" · ")}
					</div>
				</div>
			</div>
		</div>
	);
};

type Ticker = "order" | "best" | "clock";

// THE BAR ALONG THE BOTTOM, on the way to center court.
const Bar = ({
	info,
	line,
	lineKey,
	ticker,
}: {
	info: DraftNightInfo;
	line: string | undefined;
	lineKey: string;
	ticker: Ticker | undefined;
}) => {
	const [main, accent] = info.team.colors;
	const ink = inkOn(main);
	const school = schoolOf(info);
	const barRef = useEnter("bar", "translateY(100%)");
	const lineRef = useEnter(lineKey, "translateY(40%)");
	const tickerRef = useEnter(ticker ?? "", "translateX(-0.8em)");
	const next = info.night.next[0];
	return (
		<div
			ref={barRef}
			style={{
				position: "absolute",
				left: 0,
				right: 0,
				bottom: 0,
				zIndex: 2,
				fontFamily: FONT,
				fontSize: "clamp(12px, 1.9cqw, 18px)",
				lineHeight: 1.1,
				whiteSpace: "nowrap",
				pointerEvents: "none",
				boxShadow: "0 -4px 14px rgba(0,0,0,.45)",
			}}
		>
			<div style={{ display: "flex", alignItems: "stretch" }}>
				<div
					style={{
						display: "flex",
						flexDirection: "column",
						justifyContent: "center",
						padding: "0.3em 0.6em",
						background: "#0b0b10",
						color: "#fff",
						borderRight: `0.2em solid ${accent}`,
					}}
				>
					<span
						style={{
							fontSize: "0.68em",
							fontWeight: 800,
							letterSpacing: "0.08em",
							color: GOLD,
						}}
					>
						R{info.round} · P{info.pick}
					</span>
					<span
						style={{
							fontSize: "0.78em",
							fontWeight: 900,
							letterSpacing: "0.06em",
						}}
					>
						PICK IS IN
					</span>
				</div>
				<div
					style={{
						flex: 1,
						minWidth: 0,
						display: "flex",
						alignItems: "center",
						gap: "0.55em",
						padding: "0.3em 0.7em",
						background: main,
						color: ink,
						overflow: "hidden",
					}}
				>
					<span
						style={{
							flexShrink: 0,
							fontSize: "1.45em",
							fontWeight: 900,
							fontStyle: "italic",
							textTransform: "uppercase",
						}}
					>
						{info.player.name}
					</span>
					{info.player.pos ? (
						<span
							style={{ flexShrink: 0, fontWeight: 800, fontSize: "0.85em" }}
						>
							{info.player.pos}
						</span>
					) : null}
					{school ? (
						<span
							style={{
								fontSize: "0.7em",
								fontWeight: 700,
								padding: "0.15em 0.6em",
								border: `1px solid ${ink === "#fff" ? "rgba(255,255,255,.6)" : "rgba(0,0,0,.5)"}`,
								borderRadius: 999,
								minWidth: 0,
								overflow: "hidden",
								textOverflow: "ellipsis",
							}}
						>
							{school}
						</span>
					) : null}
					<TeamLogoInline
						alt={info.team.abbrev}
						imgURL={info.team.imgURL}
						imgURLSmall={info.team.imgURLSmall}
						style={{
							marginLeft: "auto",
							flexShrink: 0,
							height: "1.8em",
							width: "1.8em",
						}}
					/>
				</div>
			</div>
			<div
				style={{
					padding: "0.28em 0.8em",
					background: "rgba(10, 10, 14, 0.94)",
					color: "#fff",
					fontSize: "0.82em",
					fontWeight: 600,
					letterSpacing: "0.03em",
					minHeight: "1.65em",
					overflow: "hidden",
				}}
			>
				<div ref={lineRef}>{line ?? " "}</div>
			</div>
			{ticker ? (
				<div
					style={{
						display: "flex",
						alignItems: "center",
						gap: "0.6em",
						padding: "0.22em 0.5em",
						background: "#f3f3f5",
						color: "#111",
						fontSize: "0.72em",
						fontWeight: 700,
						overflow: "hidden",
					}}
				>
					<span
						style={{
							flexShrink: 0,
							padding: "0.18em 0.5em",
							background: ticker === "clock" ? main : "#111",
							color: ticker === "clock" ? ink : "#fff",
							fontWeight: 900,
							letterSpacing: "0.08em",
							borderRadius: 2,
						}}
					>
						{ticker === "order"
							? "DRAFT ORDER"
							: ticker === "best"
								? "BEST AVAILABLE"
								: "ON THE CLOCK"}
					</span>
					<div
						ref={tickerRef}
						style={{
							display: "flex",
							alignItems: "center",
							gap: "1em",
							minWidth: 0,
							overflow: "hidden",
						}}
					>
						{ticker === "order" ? (
							info.night.next.map((n) => (
								<span
									key={n.overall}
									style={{
										display: "inline-flex",
										alignItems: "center",
										gap: "0.3em",
										flexShrink: 0,
									}}
								>
									<span>{n.overall}.</span>
									<TeamLogoInline
										alt=""
										imgURL={n.imgURL}
										imgURLSmall={n.imgURLSmall}
										style={{ height: "1.3em", width: "1.3em" }}
									/>
									<span>{n.abbrev}</span>
								</span>
							))
						) : ticker === "best" ? (
							info.night.best.map((b) => (
								<span key={b.rank} style={{ flexShrink: 0 }}>
									{b.rank}. {b.name.toUpperCase()}
									<span style={{ fontWeight: 500, color: "#555" }}>
										{" "}
										{[b.pos, b.college].filter(Boolean).join(" · ")}
									</span>
								</span>
							))
						) : next ? (
							<span
								style={{
									display: "inline-flex",
									alignItems: "center",
									gap: "0.35em",
								}}
							>
								<span>{next.overall}.</span>
								<TeamLogoInline
									alt=""
									imgURL={next.imgURL}
									imgURLSmall={next.imgURLSmall}
									style={{ height: "1.3em", width: "1.3em" }}
								/>
								<span>
									{next.region.toUpperCase()} {next.name.toUpperCase()}
								</span>
							</span>
						) : null}
					</div>
				</div>
			) : null}
		</div>
	);
};

export const DraftBroadcast = ({
	info,
	feed,
}: {
	info: DraftNightInfo;
	feed: DraftFeed;
}) => {
	const { units } = useLocal(["units"]);
	const { night } = info;
	const lines = [
		`${night.age} years old · ${getHeightString(night.hgt, units)} · ${units === "metric" ? `${Math.round(night.weight / 2.205)} kg` : `${night.weight} lbs`}`,
		...draftFacts({
			relatives: night.relatives,
			rank: night.rank,
			born: night.born,
			via: night.via,
		}),
	];
	const hasOrder = night.next.length > 0;
	const hasBest = night.best.length > 0;

	// What is shown, as one string, so the page only draws again when it
	// changes - not every frame the scene moves.
	const shown = useSyncExternalStore(feed.subscribe, () => {
		const s = feed.get();
		const line =
			s.t < LINE_FROM
				? -1
				: Math.min(
						lines.length - 1,
						Math.floor((s.t - LINE_FROM) / LINE_EVERY),
					);
		let ticker: Ticker =
			Math.floor(s.t / TICKER_EVERY) % 2 === 0 ? "order" : "best";
		if (ticker === "order" && !hasOrder) {
			ticker = "best";
		} else if (ticker === "best" && !hasBest) {
			ticker = "order";
		}
		if (s.over && hasOrder) {
			ticker = "clock";
		}
		return `${s.cover ? 1 : 0}|${s.run}|${line}|${ticker}`;
	});
	const [cover, run, lineIndex, ticker] = shown.split("|") as [
		string,
		string,
		string,
		Ticker,
	];
	const i = Number(lineIndex);
	return (
		<>
			{cover === "0" ? (
				<Bar
					key={`bar${run}`}
					info={info}
					line={i >= 0 ? lines[i] : undefined}
					lineKey={lineIndex}
					ticker={!hasOrder && !hasBest ? undefined : ticker}
				/>
			) : null}
			<Podium key={`podium${run}`} info={info} shown={cover === "1"} />
		</>
	);
};
