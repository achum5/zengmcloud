import {
	useEffect,
	useLayoutEffect,
	type CSSProperties,
	type ReactNode,
} from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import type { ScoreBugPiece, ScoreBugStyle } from "../../../../common/types.ts";
import type { BugRefs, BugTeam } from "./ScoreBug.tsx";
import { bugShown, writeBug, type BugNow } from "./bugWriter.ts";
import { lightness } from "./figure.ts";

// THE LEAGUE'S OWN SCORE BUG (see ScoreBugStyle): its pieces placed in the
// bug's own units, which scale with the picture. The clocks, the fouls, the
// ball (and football's down and distance) are the same refs the default bug
// has, written by the frame loop - or, previewed, written once from
// `sample`.

export type CustomBugTeam = BugTeam & { region?: string; name?: string };

const MAX_TIMEOUTS = 7;
// The smallest a bug is drawn, px - at least this wide, and at least this
// tall: a corner box or a bar across the bottom the size it is on TV is too
// small to read on a phone.
const MIN_WIDTH = 150;
const MIN_HEIGHT = 32;

// A team's color written into CSS: "home0" alone, or in a gradient, a
// border, a shadow ("linear-gradient(90deg, away0, away1)") - or
// "homeInk", white or black, whichever reads on its main color.
const TEAM_COLOR = /(?<![\w./-])(away|home)([0-2]|Ink)(?![\w.-])/g;

// "Q3" as "3rd" ("OT", "2OT" as they are).
const ordinal = (period: string) => {
	const m = /^Q(\d+)$/.exec(period);
	if (!m) {
		return { n: period, suffix: "" };
	}
	const n = Number(m[1]);
	const suffix =
		n % 100 >= 11 && n % 100 <= 13
			? "th"
			: (["th", "st", "nd", "rd"][n % 10] ?? "th");
	return { n: m[1]!, suffix };
};

const FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";
// A font named on its own ("Gotham Bold" - one the bug brings, say) quoted,
// falling back on the usual; a CSS list, as given.
const family = (f: string | undefined) =>
	f === undefined || /["',]/.test(f) ? f : `"${f}", ${FONT}`;

// The fonts bugs bring, added to the page once each.
const fontsAdded = new Set<string>();
const addFonts = (fonts: ScoreBugStyle["fonts"]) => {
	for (const { name, src } of fonts ?? []) {
		const key = `${name}\n${src}`;
		if (fontsAdded.has(key) || typeof FontFace === "undefined") {
			continue;
		}
		fontsAdded.add(key);
		const face = new FontFace(name, `url("${src.replaceAll('"', "%22")}")`);
		face
			.load()
			.then(() => {
				document.fonts.add(face);
			})
			.catch(() => {
				// The lettering falls back.
			});
	}
};

export const CustomScoreBug = ({
	style,
	away,
	home,
	quarter,
	totalTimeouts,
	refs,
	hidden,
	sample,
}: {
	style: ScoreBugStyle;
	away: CustomBugTeam | undefined;
	home: CustomBugTeam | undefined;
	quarter: string;
	totalTimeouts: number | undefined;
	refs: BugRefs;
	hidden?: boolean;
	// For a preview: what the frame loop would write.
	sample?: BugNow;
}) => {
	useLayoutEffect(() => {
		if (sample) {
			writeBug(refs, bugShown(), sample);
		}
	});

	useEffect(() => {
		addFonts(style.fonts);
	}, [style.fonts]);

	const W = style.width;
	const H = style.height;
	// A length in the bug's units, as a share of its width.
	const u = (n: number) => `${(n / W) * 100}cqw`;
	const team = (side: "away" | "home") => (side === "away" ? away : home);
	// "away0".."home2": that side's colors, wherever they're written;
	// anything else, as given.
	const teamColor = (side: "away" | "home", which: string) => {
		const colors = team(side)?.colors;
		if (which === "Ink") {
			return colors && lightness(colors[0]) > 0.62 ? "#111111" : "#ffffff";
		}
		return colors?.[Number(which)];
	};
	const color = (c: string | undefined) => {
		const m = c === undefined ? null : /^(away|home)([0-2]|Ink)$/.exec(c);
		if (m) {
			return teamColor(m[1] as "away" | "home", m[2]!);
		}
		return c?.replace(
			TEAM_COLOR,
			(_, side: "away" | "home", which: string) =>
				teamColor(side, which) ?? "transparent",
		);
	};
	const justify = (p: ScoreBugPiece): CSSProperties["justifyContent"] =>
		p.align === "right"
			? "flex-end"
			: p.align === "center"
				? "center"
				: "flex-start";

	const piece = (p: ScoreBugPiece, i: number) => {
		const onMark =
			p.show === "shotClock" ||
			p.show === "awayBonus" ||
			p.show === "homeBonus";
		const box: CSSProperties = {
			position: "absolute",
			left: `${(p.x / W) * 100}%`,
			top: `${(p.y / H) * 100}%`,
			width: `${(p.w / W) * 100}%`,
			height: `${(p.h / H) * 100}%`,
			display: "flex",
			alignItems: "center",
			justifyContent: justify(p),
			color: color(p.color),
			// (The shot clock's and a bonus mark's on the mark itself, gone with
			// it when it's off.)
			background: onMark ? undefined : color(p.background),
			fontSize: u(p.size ?? 16),
			fontWeight: p.weight,
			fontFamily: family(p.font),
			fontStyle: p.italic ? "italic" : undefined,
			letterSpacing: p.spacing === undefined ? undefined : u(p.spacing),
			textTransform: p.upper ? "uppercase" : undefined,
			borderRadius: p.radius === undefined ? undefined : u(p.radius),
			border: onMark ? undefined : color(p.border),
			clipPath: p.clip,
			textShadow: color(p.shadow),
			mixBlendMode: p.blend as CSSProperties["mixBlendMode"],
			opacity: p.opacity,
			overflow: "hidden",
			whiteSpace: "nowrap",
			fontVariantNumeric: "tabular-nums",
			boxSizing: "border-box",
		};
		const side = p.show.startsWith("away")
			? "away"
			: p.show.startsWith("home")
				? "home"
				: undefined;
		const t = side ? team(side) : undefined;
		const k = side === "away" ? 0 : 1;
		let inner: ReactNode = null;
		switch (p.show) {
			case "awayLogo":
			case "homeLogo":
				inner = (
					<TeamLogoInline
						alt={t?.abbrev}
						imgURL={t?.imgURL}
						imgURLSmall={t?.imgURLSmall}
						includePlaceholderIfNoLogo
						style={{ height: "100%", width: "100%", objectFit: "contain" }}
					/>
				);
				break;
			case "awayAbbrev":
			case "homeAbbrev":
				inner = t?.abbrev ?? "";
				break;
			case "awayRegion":
			case "homeRegion":
				inner = t?.region ?? "";
				break;
			case "awayName":
			case "homeName":
				inner = t?.name ?? "";
				break;
			case "awayScore":
			case "homeScore":
				inner = t?.pts ?? 0;
				break;
			case "awayRecord":
			case "homeRecord":
				inner = t?.record ?? "";
				break;
			case "homeLead": {
				const lead = (home?.pts ?? 0) - (away?.pts ?? 0);
				const n = lead > 0 ? `+${lead}` : String(lead);
				inner = p.text === undefined ? n : p.text.replace("{n}", n);
				break;
			}
			case "awayFouls":
			case "homeFouls":
				inner = (
					<span ref={refs.fouls[k]} data-fmt={p.text} data-limit={p.limit} />
				);
				break;
			case "awayBonus":
			case "homeBonus":
				// (Its box, background and border, gone with it.)
				inner = (
					<span
						ref={refs.bonus?.[k]}
						style={{
							visibility: "hidden",
							display: "flex",
							alignItems: "center",
							justifyContent: justify(p),
							width: "100%",
							height: "100%",
							background: color(p.background),
							border: color(p.border),
							borderRadius: "inherit",
							boxSizing: "border-box",
						}}
					>
						<span
							style={
								p.rotate === undefined
									? undefined
									: { transform: `rotate(${p.rotate}deg)`, flex: "none" }
							}
						>
							{p.text ?? "BONUS"}
						</span>
					</span>
				);
				break;
			case "awayBall":
			case "homeBall":
				inner = (
					<span
						ref={refs.ball[k]}
						style={{
							width: 0,
							height: 0,
							borderTop: `${u(p.h * 0.3)} solid transparent`,
							borderBottom: `${u(p.h * 0.3)} solid transparent`,
							borderLeft: `${u(p.h * 0.45)} solid currentColor`,
							visibility: "hidden",
						}}
					/>
				);
				break;
			case "awayTimeouts":
			case "homeTimeouts": {
				const left = t?.timeouts;
				if (left !== undefined && p.text !== undefined) {
					inner = p.text.replace("{n}", String(left));
				} else if (left !== undefined && totalTimeouts !== undefined) {
					const n = Math.min(MAX_TIMEOUTS, totalTimeouts);
					inner = (
						<span
							style={{
								display: "flex",
								gap: p.spacing === undefined ? "8%" : u(p.spacing),
								width: "100%",
								height: "100%",
							}}
						>
							{Array.from({ length: n }, (_, j) => (
								<span
									key={j}
									style={{
										flex: 1,
										borderRadius: 1,
										background:
											j < left
												? (color(p.color) ?? "#f2c14e")
												: (color(p.off) ?? "rgba(255,255,255,.18)"),
									}}
								/>
							))}
						</span>
					);
				}
				break;
			}
			case "period": {
				if (!p.ordinal) {
					inner = quarter;
					break;
				}
				const { n, suffix } = ordinal(quarter);
				inner =
					p.ordinal === "plain" || !suffix ? (
						`${n}${suffix}`
					) : (
						<>
							{n}
							<span
								style={{
									fontSize: p.ordinal === "sup" ? "0.55em" : "0.7em",
									alignSelf: p.ordinal === "sup" ? "flex-start" : undefined,
									marginTop: p.ordinal === "sup" ? "0.32em" : undefined,
									marginLeft: "0.04em",
								}}
							>
								{suffix}
							</span>
						</>
					);
				break;
			}
			case "clock":
				inner = <span ref={refs.clock} data-colon={p.colon ? "" : undefined} />;
				break;
			case "shotClock":
				inner = (
					<span
						ref={refs.shot}
						data-colon={p.colon ? "shot" : undefined}
						data-low={color(p.low)}
						data-low-background={color(p.lowBackground)}
						data-background={color(p.background)}
						style={{
							display: "none",
							width: "100%",
							height: "100%",
							alignItems: "center",
							justifyContent: justify(p),
							borderRadius: "inherit",
							background: color(p.background),
						}}
					/>
				);
				break;
			case "down":
				inner = <span ref={refs.down} />;
				break;
			case "text":
				inner = p.text ?? "";
				break;
			case "image":
				inner = p.image ? (
					<img
						src={p.image}
						alt=""
						style={{
							width: "100%",
							height: "100%",
							objectFit: p.fit ?? "contain",
						}}
					/>
				) : null;
				break;
			case "box":
				break;
		}
		// (Turned, or made bigger, within its box.)
		const turned =
			(p.rotate !== undefined &&
				p.show !== "awayBonus" &&
				p.show !== "homeBonus") ||
			p.zoom !== undefined;
		if (inner !== null && turned) {
			inner = (
				<span
					style={{
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
						flex: "none",
						...(p.show.endsWith("Logo")
							? { width: "100%", height: "100%" }
							: {}),
						// (Timeouts turned on their side still fill the box.)
						...(p.show.endsWith("Timeouts") &&
						p.text === undefined &&
						p.rotate !== undefined &&
						Math.abs(p.rotate) % 180 === 90
							? { width: u(p.h), height: u(p.w) }
							: {}),
						transform: [
							p.rotate === undefined ? "" : `rotate(${p.rotate}deg)`,
							p.zoom === undefined ? "" : `scale(${p.zoom})`,
						].join(" "),
					}}
				>
					{inner}
				</span>
			);
		}
		// Keyed by what it shows too: a clock or fouls span written by the
		// frame loop is never reused for another piece.
		return (
			<div key={`${i}-${p.show}`} style={box}>
				{inner}
			</div>
		);
	};

	const span = style.span ?? 0.6;
	const place = style.place ?? "center";
	// (Never so small on a phone that it can't be read.)
	const width = `min(96%, max(${span * 100}%, ${Math.max(MIN_WIDTH, (MIN_HEIGHT * W) / H)}px))`;
	// (And never pushed off the side by being made bigger.)
	const edge = `min(${(style.edge ?? 0.016) * 100}cqw, calc(98% - ${width}))`;
	return (
		<div
			style={{
				display: hidden ? "none" : "block",
				position: "absolute",
				bottom: `${(style.bottom ?? 0.016) * 100}cqw`,
				...(place === "center"
					? { left: "50%", transform: "translateX(-50%)" }
					: place === "left"
						? { left: edge }
						: { right: edge }),
				width,
			}}
		>
			<div
				style={{
					position: "relative",
					width: "100%",
					aspectRatio: `${W} / ${H}`,
					containerType: "inline-size",
					background: style.image
						? `center / 100% 100% no-repeat url("${style.image.replaceAll('"', "%22")}")${style.background ? `, ${color(style.background)}` : ""}`
						: color(style.background),
					borderRadius:
						style.radius === undefined
							? undefined
							: `${(style.radius / W) * 100}cqw`,
					overflow: "hidden",
					lineHeight: 1,
					fontFamily: family(style.font) ?? FONT,
					boxShadow: color(style.shadow) ?? "0 3px 10px rgba(0,0,0,.5)",
				}}
			>
				{style.pieces.map(piece)}
			</div>
		</div>
	);
};
