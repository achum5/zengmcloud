import { useLayoutEffect, type CSSProperties, type ReactNode } from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import type { ScoreBugPiece, ScoreBugStyle } from "../../../../common/types.ts";
import type { BugRefs, BugTeam } from "./ScoreBug.tsx";

// THE LEAGUE'S OWN SCORE BUG (see ScoreBugStyle): its pieces placed in the
// bug's own units, which scale with the picture. The clocks, the fouls and
// the ball are the same refs the default bug has, written by the court's
// frame loop - or, previewed, written once from `sample`.

export type CustomBugTeam = BugTeam & { region?: string; name?: string };

const MAX_TIMEOUTS = 7;

// How tall the bug stands, as a share (in cqw) of the picture's width.
export const customBugHeight = (style: ScoreBugStyle) =>
	(style.span ?? 0.6) * 100 * (style.height / style.width);

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
	sample?: {
		clock: string;
		shot: string;
		fouls: [string, string];
		ball: 0 | 1;
	};
}) => {
	useLayoutEffect(() => {
		if (!sample) {
			return;
		}
		if (refs.clock.current) {
			refs.clock.current.textContent = sample.clock;
		}
		if (refs.shot.current) {
			refs.shot.current.textContent = sample.shot;
			refs.shot.current.style.display = "flex";
		}
		refs.fouls.forEach((r, i) => {
			if (r.current) {
				r.current.textContent = sample.fouls[i]!;
			}
		});
		refs.ball.forEach((r, i) => {
			if (r.current) {
				r.current.style.visibility = i === sample.ball ? "visible" : "hidden";
			}
		});
	});

	const W = style.width;
	const H = style.height;
	// A length in the bug's units, as a share of its width.
	const u = (n: number) => `${(n / W) * 100}cqw`;
	const team = (side: "away" | "home") => (side === "away" ? away : home);
	// "away0".."home2": that side's colors; anything else, as given.
	const color = (c: string | undefined) => {
		const m = c === undefined ? null : /^(away|home)([0-2])$/.exec(c);
		return m ? team(m[1] as "away" | "home")?.colors?.[Number(m[2])] : c;
	};

	const piece = (p: ScoreBugPiece, i: number) => {
		const box: CSSProperties = {
			position: "absolute",
			left: `${(p.x / W) * 100}%`,
			top: `${(p.y / H) * 100}%`,
			width: `${(p.w / W) * 100}%`,
			height: `${(p.h / H) * 100}%`,
			display: "flex",
			alignItems: "center",
			justifyContent:
				p.align === "right"
					? "flex-end"
					: p.align === "center"
						? "center"
						: "flex-start",
			color: color(p.color),
			background: color(p.background),
			fontSize: u(p.size ?? 16),
			fontWeight: p.weight,
			fontFamily: p.font,
			fontStyle: p.italic ? "italic" : undefined,
			borderRadius: p.radius === undefined ? undefined : u(p.radius),
			opacity: p.opacity,
			overflow: "hidden",
			whiteSpace: "nowrap",
			fontVariantNumeric: "tabular-nums",
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
			case "awayFouls":
			case "homeFouls":
				inner = <span ref={refs.fouls[k]} />;
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
				if (left !== undefined && totalTimeouts !== undefined) {
					const n = Math.min(MAX_TIMEOUTS, totalTimeouts);
					inner = (
						<span
							style={{
								display: "flex",
								gap: "8%",
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
												: "rgba(255,255,255,.18)",
									}}
								/>
							))}
						</span>
					);
				}
				break;
			}
			case "period":
				inner = quarter;
				break;
			case "clock":
				inner = <span ref={refs.clock} />;
				break;
			case "shotClock":
				inner = <span ref={refs.shot} style={{ display: "none" }} />;
				break;
			case "text":
				inner = p.text ?? "";
				break;
			case "image":
				inner = p.image ? (
					<img
						src={p.image}
						alt=""
						style={{ width: "100%", height: "100%", objectFit: "contain" }}
					/>
				) : null;
				break;
			case "box":
				break;
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
	return (
		<div
			style={{
				display: hidden ? "none" : "block",
				position: "absolute",
				bottom: "1.6cqw",
				...(place === "center"
					? { left: "50%", transform: "translateX(-50%)" }
					: place === "left"
						? { left: "1.6cqw" }
						: { right: "1.6cqw" }),
				width: `${span * 100}%`,
			}}
		>
			<div
				style={{
					position: "relative",
					width: "100%",
					aspectRatio: `${W} / ${H}`,
					containerType: "inline-size",
					background: style.image
						? `center / 100% 100% no-repeat url("${style.image.replaceAll('"', "%22")}")${style.background ? `, ${style.background}` : ""}`
						: style.background,
					borderRadius:
						style.radius === undefined
							? undefined
							: `${(style.radius / W) * 100}cqw`,
					overflow: "hidden",
					lineHeight: 1,
					fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
					boxShadow: "0 3px 10px rgba(0,0,0,.5)",
				}}
			>
				{style.pieces.map(piece)}
			</div>
		</div>
	);
};
