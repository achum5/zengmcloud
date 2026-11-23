import { useEffect, useRef, useSyncExternalStore } from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { getHeightString } from "../../../components/Height.tsx";
import { useLocal } from "../../../util/local.ts";
import {
	announcement,
	draftFacts,
	ordinal,
} from "../../../../common/draftNight.ts";
import { DRAFT_CALL } from "./draftStage.ts";
import { lightness } from "./figure.ts";

// DRAFT NIGHT, AS IT IS BROADCAST, over a pick made on the stage (see
// draftStage.ts, HeadlineStage.tsx): THE PICK IS IN over the team's colors
// while the room is got ready; then the broadcast's bar along the bottom -
// the pick, the team and SELECTION while the commissioner reads it out, his
// words under it; and once his name is read, the name, the school and a line
// that goes through his age and size and the true things about him, and
// under that the draft order and the best still on the board. Once he is up
// on the stage, the next team on the clock.
//
// Graphics only: nothing here touches the draft.

const FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";
const GOLD = "#f2c14e";

// How long THE PICK IS IN is up before the room is shown (ms).
export const PRE_ROLL_MS = 1600;
// When the commissioner's words come up under SELECTION, and his name (ms of
// the scene - see draftStage.ts).
const WORDS_AT = DRAFT_CALL.podium;
const NAME_AT = DRAFT_CALL.call;
// When the line under his name first comes up, and how long each stays (ms
// after his name).
const LINE_FROM = 600;
const LINE_EVERY = 3000;
// How long each of the two tickers runs before the other (ms of the scene).
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

// THE PICK IS IN: the team's colors over everything, the pick and the team.
const Podium = ({ info, shown }: { info: DraftNightInfo; shown: boolean }) => {
	const [main, accent] = info.team.colors;
	const ink = inkOn(main);
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
					"@keyframes dn-in{from{opacity:0;transform:translateX(-1.2em)}to{opacity:1;transform:none}}"
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
			</div>
		</div>
	);
};

type Ticker = "order" | "best" | "clock";

// THE BAR ALONG THE BOTTOM: SELECTION while it is read out, then his name.
const Bar = ({
	info,
	named,
	line,
	lineKey,
	ticker,
}: {
	info: DraftNightInfo;
	named: boolean;
	line: string | undefined;
	lineKey: string;
	ticker: Ticker | undefined;
}) => {
	const [main, accent] = info.team.colors;
	const ink = inkOn(main);
	const school = schoolOf(info);
	const barRef = useEnter("bar", "translateY(100%)");
	const nameRef = useEnter(named ? "name" : "selection", "translateX(-0.6em)");
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
					<div
						ref={nameRef}
						style={{
							display: "flex",
							alignItems: "center",
							gap: "0.55em",
							minWidth: 0,
						}}
					>
						{named ? (
							<>
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
										style={{
											flexShrink: 0,
											fontWeight: 800,
											fontSize: "0.85em",
										}}
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
							</>
						) : (
							<>
								<span
									style={{
										flexShrink: 0,
										fontWeight: 700,
										fontSize: "0.8em",
										letterSpacing: "0.08em",
										textTransform: "uppercase",
										opacity: 0.85,
									}}
								>
									{info.team.region}
								</span>
								<span
									style={{
										flexShrink: 0,
										fontSize: "1.45em",
										fontWeight: 900,
										fontStyle: "italic",
										letterSpacing: "0.2em",
									}}
								>
									SELECTION
								</span>
								<span
									style={{
										flexShrink: 0,
										fontWeight: 700,
										fontSize: "0.8em",
										letterSpacing: "0.08em",
										textTransform: "uppercase",
										opacity: 0.85,
									}}
								>
									{info.team.name}
								</span>
							</>
						)}
					</div>
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
	const words = `${announcement({
		overall: night.overall,
		season: info.season,
		team: `${info.team.region} ${info.team.name}`,
	})}…`;
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
		const named = s.t >= NAME_AT;
		const after = s.t - NAME_AT;
		// (Before his name: the commissioner's words once he starts - -2.)
		const line = !named
			? s.t >= WORDS_AT
				? -2
				: -1
			: after < LINE_FROM
				? -1
				: Math.min(
						lines.length - 1,
						Math.floor((after - LINE_FROM) / LINE_EVERY),
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
		return `${s.cover ? 1 : 0}|${s.run}|${named ? 1 : 0}|${line}|${ticker}`;
	});
	const [cover, run, named, lineIndex, ticker] = shown.split("|") as [
		string,
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
					named={named === "1"}
					line={i >= 0 ? lines[i] : i === -2 ? words : undefined}
					lineKey={lineIndex}
					ticker={!hasOrder && !hasBest ? undefined : ticker}
				/>
			) : null}
			<Podium key={`podium${run}`} info={info} shown={cover === "1"} />
		</>
	);
};

// ---- between picks ----------------------------------------------------------

export type DraftClockInfo = {
	season: number;
	round: number;
	pick: number;
	overall: number;
	team: TeamInfo;
	next: (TeamInfo & { overall: number })[];
	best: { rank: number; name: string; pos?: string; college?: string }[];
	// Those the cameras go to, in the order they go to them.
	prospects: {
		name: string;
		pos?: string;
		college?: string;
		rank?: number;
	}[];
};

// ON THE CLOCK: the bar along the bottom while a team is up - the pick and
// the team, a line under it (who the cameras are on, when they are on one of
// the best left), and the picks after it and the best still on the board
// in turn.
export const DraftClockBroadcast = ({
	info,
	feed,
	prospectAt,
}: {
	info: DraftClockInfo;
	feed: DraftFeed;
	// Which of the prospects the cameras are on at t, if any.
	prospectAt: (t: number) => number | undefined;
}) => {
	const shown = useSyncExternalStore(feed.subscribe, () => {
		const { t } = feed.get();
		const p = prospectAt(t);
		let ticker: "next" | "best" =
			Math.floor(t / TICKER_EVERY) % 2 === 0 ? "next" : "best";
		if (ticker === "next" && info.next.length === 0) {
			ticker = "best";
		} else if (ticker === "best" && info.best.length === 0) {
			ticker = "next";
		}
		return `${p ?? -1}|${ticker}`;
	});
	const [pText, ticker] = shown.split("|") as [string, "next" | "best"];
	const p = info.prospects[Number(pText)];
	const [main, accent] = info.team.colors;
	const ink = inkOn(main);
	const line = p
		? [
				p.name.toUpperCase(),
				[p.pos, p.college].filter(Boolean).join(" · "),
				p.rank !== undefined ? `No. ${p.rank} on the big board` : undefined,
			]
				.filter(Boolean)
				.join("  ·  ")
		: `The ${info.team.region} ${info.team.name} are on the clock with the ${ordinal(info.overall)} pick`;
	const barRef = useEnter("bar", "translateY(100%)");
	const lineRef = useEnter(pText, "translateY(40%)");
	const tickerRef = useEnter(ticker, "translateX(-0.8em)");
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
						ON THE CLOCK
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
							fontWeight: 700,
							fontSize: "0.8em",
							letterSpacing: "0.08em",
							textTransform: "uppercase",
							opacity: 0.85,
						}}
					>
						{info.team.region}
					</span>
					<span
						style={{
							minWidth: 0,
							overflow: "hidden",
							textOverflow: "ellipsis",
							fontSize: "1.45em",
							fontWeight: 900,
							fontStyle: "italic",
							textTransform: "uppercase",
						}}
					>
						{info.team.name}
					</span>
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
					textOverflow: "ellipsis",
				}}
			>
				<div ref={lineRef}>
					{p ? (
						<span
							style={{
								marginRight: "0.6em",
								padding: "0.05em 0.45em",
								background: GOLD,
								color: "#111",
								fontWeight: 900,
								fontSize: "0.85em",
								letterSpacing: "0.08em",
								borderRadius: 2,
							}}
						>
							GREEN ROOM
						</span>
					) : null}
					{line}
				</div>
			</div>
			{info.next.length > 0 || info.best.length > 0 ? (
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
							background: "#111",
							color: "#fff",
							fontWeight: 900,
							letterSpacing: "0.08em",
							borderRadius: 2,
						}}
					>
						{ticker === "next" ? "UP NEXT" : "BEST AVAILABLE"}
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
						{ticker === "next"
							? info.next.map((n) => (
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
							: info.best.map((b) => (
									<span key={b.rank} style={{ flexShrink: 0 }}>
										{b.rank}. {b.name.toUpperCase()}
										<span style={{ fontWeight: 500, color: "#555" }}>
											{" "}
											{[b.pos, b.college].filter(Boolean).join(" · ")}
										</span>
									</span>
								))}
					</div>
				</div>
			) : null}
		</div>
	);
};
