import {
	useEffect,
	useId,
	useRef,
	type CSSProperties,
	type ReactNode,
} from "react";
import type {
	RecapCardTeam,
	RecapLeader,
} from "../../../worker/util/nightlyRecap.ts";
import { lightness, shade } from "../../views/LiveGame/court3d/figure.ts";
import { PlayerPicture } from "../PlayerPicture.tsx";

// THE NIGHTLY RECAP'S GRAPHICS, drawn over and between the 3D court the way
// the NBA's recap draws its own (see the design's reference frames): the
// screen split in the two teams' colors with their logos cut off at the
// edges, the abbreviations huge and faint behind, and a blue disc ringed in
// white dashes holding the title. The away team on the left, as in "LAL @
// BOS". Sized by the picture's width (cqw), every size with a floor for a
// phone.

const FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";
const DISC = "#1d3f9e";
const DISC_DARK = "#122a73";

const inkOn = (c: string) => (lightness(c) > 0.62 ? "#101014" : "#fff");

// Tall, heavy type the way the broadcast sets its titles: any system font
// squeezed to a condensed cut.
const Condensed = ({
	children,
	style,
}: {
	children: ReactNode;
	style?: CSSProperties;
}) => (
	<span
		style={{
			display: "inline-block",
			fontWeight: 900,
			transform: "scaleX(0.8)",
			transformOrigin: "center",
			lineHeight: 0.92,
			whiteSpace: "nowrap",
			...style,
		}}
	>
		{children}
	</span>
);

// In with a scale and a fade, once, as a card comes up.
const usePop = (ms = 380, delay = 0, from = 0.6) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		ref.current?.animate?.(
			[
				{ opacity: 0, transform: `scale(${from})` },
				{ opacity: 1, transform: "scale(1)" },
			],
			{
				duration: ms,
				delay,
				easing: "cubic-bezier(.2,.9,.25,1.15)",
				fill: "backwards",
			},
		);
	}, [ms, delay, from]);
	return ref;
};

const useSlide = (dx: number, delay = 0) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		ref.current?.animate?.(
			[
				{ opacity: 0, transform: `translateX(${dx}cqw)` },
				{ opacity: 1, transform: "translateX(0)" },
			],
			{
				duration: 520,
				delay,
				easing: "cubic-bezier(.2,.8,.2,1)",
				fill: "backwards",
			},
		);
	}, [dx, delay]);
	return ref;
};

const Logo = ({ team, side }: { team: RecapCardTeam; side: "l" | "r" }) => {
	const ref = useSlide(side === "l" ? -12 : 12, 60);
	return (
		<div
			ref={ref}
			style={{
				position: "absolute",
				top: "50%",
				[side === "l" ? "left" : "right"]: "-11cqw",
				width: "38cqw",
				height: "38cqw",
				marginTop: "-19cqw",
				borderRadius: "50%",
				background: shade(team.colors[0], -0.18),
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				overflow: "hidden",
			}}
		>
			{team.imgURL ? (
				<img
					alt=""
					src={team.imgURL}
					style={{ width: "78%", height: "78%", objectFit: "contain" }}
				/>
			) : (
				<Condensed style={{ color: inkOn(team.colors[0]), fontSize: "11cqw" }}>
					{team.abbrev}
				</Condensed>
			)}
		</div>
	);
};

// The two halves, the faint abbreviations and the logos: the ground both
// cards stand on.
const Split = ({
	away,
	home,
	children,
}: {
	away: RecapCardTeam;
	home: RecapCardTeam;
	children: ReactNode;
}) => (
	<div
		style={{
			position: "absolute",
			inset: 0,
			overflow: "hidden",
			fontFamily: FONT,
			background: `linear-gradient(90deg, ${away.colors[0]} 0 50%, ${home.colors[0]} 50% 100%)`,
			pointerEvents: "none",
		}}
	>
		{[away, home].map((t, i) => (
			<div
				key={i}
				aria-hidden
				style={{
					position: "absolute",
					top: "-3cqw",
					[i === 0 ? "left" : "right"]: "2cqw",
					width: "46cqw",
					textAlign: i === 0 ? "left" : "right",
					color: inkOn(t.colors[0]),
					opacity: 0.1,
					fontSize: "17cqw",
				}}
			>
				<Condensed>{t.abbrev}</Condensed>
			</div>
		))}
		<Logo team={away} side="l" />
		<Logo team={home} side="r" />
		{children}
	</div>
);

// A team's tag by the title: its abbreviation, and its record.
const Tag = ({
	team,
	label,
	record,
	side,
	delay,
}: {
	team: RecapCardTeam;
	label: string;
	record: string;
	side: "l" | "r";
	delay: number;
}) => {
	const ref = useSlide(side === "l" ? -4 : 4, delay);
	const bg = shade(team.colors[0], -0.22);
	return (
		<div
			ref={ref}
			style={{
				display: "flex",
				flexDirection: "column",
				alignItems: "center",
				gap: "0.35em",
				padding: "0.55em 0.7em 0.5em",
				minWidth: "4.6em",
				borderRadius: "0.35em",
				background: bg,
				color: inkOn(bg),
				fontSize: "clamp(9px, 1.9cqw, 22px)",
				boxShadow: "0 0.3em 1em rgba(0,0,0,.35)",
			}}
		>
			<Condensed style={{ fontSize: "2.1em" }}>{team.abbrev}</Condensed>
			{label ? (
				<span
					style={{
						fontSize: "0.62em",
						fontWeight: 800,
						letterSpacing: "0.12em",
						opacity: 0.85,
					}}
				>
					{label}
				</span>
			) : null}
			<Condensed style={{ fontSize: "1.6em" }}>{record}</Condensed>
		</div>
	);
};

const recordLabel = (playoffs: boolean) => (playoffs ? "SERIES" : "SEASON");

// GAME RECAP: the disc between the two teams, each with its record going in.
export const GameRecapCard = ({
	away,
	home,
	playoffs,
}: {
	away: RecapCardTeam;
	home: RecapCardTeam;
	playoffs: boolean;
}) => {
	const disc = usePop(420, 120, 0.4);
	return (
		<Split away={away} home={home}>
			<div
				style={{
					position: "absolute",
					inset: 0,
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					gap: "2.2cqw",
				}}
			>
				<Tag
					team={away}
					label={recordLabel(playoffs)}
					record={away.before}
					side="l"
					delay={320}
				/>
				<div
					ref={disc}
					style={{
						width: "34cqw",
						height: "34cqw",
						maxHeight: "min(34cqw, 92%)",
						maxWidth: "min(34cqw, 92%)",
						borderRadius: "50%",
						background: `radial-gradient(circle at 50% 40%, ${DISC} 0 62%, ${DISC_DARK} 100%)`,
						boxShadow:
							"0 0 0 0.45cqw #fff, 0 0 0 0.9cqw rgba(0,0,0,.25), 0 1.4cqw 4cqw rgba(0,0,0,.45)",
						outline: "0.35cqw dashed rgba(255,255,255,.95)",
						outlineOffset: "-1.6cqw",
						display: "flex",
						flexDirection: "column",
						alignItems: "center",
						justifyContent: "center",
						color: "#fff",
						fontSize: "8.4cqw",
						textShadow: "0 0.08em 0.25em rgba(0,0,0,.35)",
					}}
				>
					<Condensed>GAME</Condensed>
					<Condensed>RECAP</Condensed>
				</div>
				<Tag
					team={home}
					label={recordLabel(playoffs)}
					record={home.before}
					side="r"
					delay={320}
				/>
			</div>
		</Split>
	);
};

const finalLabel = (overtimes: number) =>
	overtimes <= 0
		? "FINAL"
		: overtimes === 1
			? "FINAL/OT"
			: `FINAL/${overtimes}OT`;

const splitName = (name: string): [string, string] => {
	const n = name.trim();
	const i = n.indexOf(" ");
	return i < 0 ? ["", n] : [n.slice(0, i), n.slice(i + 1)];
};

// His head and shoulders, as the broadcast's card shows a player: a drawn
// face is 400 x 600, his hair from about 55 and his shirt from 300, so the
// box takes it from HEAD_TOP down to its foot, centered. A photo fills the
// box from its top.
const HEAD_TOP = 40;
const HEAD_W = 3.2;
const HEAD_H = 3.7;
const Headshot = ({ p, team }: { p: RecapLeader; team: RecapCardTeam }) => {
	const k = HEAD_H / (600 - HEAD_TOP);
	return (
		<div
			style={{
				position: "relative",
				width: `${HEAD_W}em`,
				height: `${HEAD_H}em`,
				flex: "0 0 auto",
				alignSelf: "flex-end",
				overflow: "hidden",
			}}
		>
			{p.imgURL ? (
				<img
					alt=""
					src={p.imgURL}
					style={{
						width: "100%",
						height: "100%",
						objectFit: "cover",
						objectPosition: "center top",
					}}
				/>
			) : p.face ? (
				<div
					style={{
						position: "absolute",
						width: `${400 * k}em`,
						height: `${600 * k}em`,
						left: `${(HEAD_W - 400 * k) / 2}em`,
						top: `${-HEAD_TOP * k}em`,
					}}
				>
					<PlayerPicture
						face={p.face}
						colors={team.colors}
						jersey={team.jersey}
					/>
				</div>
			) : null}
		</div>
	);
};

const Leader = ({
	p,
	team,
	side,
}: {
	p: RecapLeader;
	team: RecapCardTeam;
	side: "l" | "r";
}) => {
	const [first, last] = splitName(p.name);
	const ink = inkOn(team.colors[0]);
	const face = <Headshot p={p} team={team} />;
	const stat = (n: number, label: string) => (
		<div
			style={{
				display: "flex",
				flexDirection: "column",
				alignItems: "center",
				minWidth: "1.9em",
			}}
		>
			<Condensed style={{ fontSize: "1.55em" }}>{n}</Condensed>
			<span
				style={{
					fontSize: "0.5em",
					fontWeight: 800,
					letterSpacing: "0.1em",
					opacity: 0.8,
					marginTop: "0.2em",
				}}
			>
				{label}
			</span>
		</div>
	);
	return (
		<div
			style={{
				display: "flex",
				flexDirection: side === "l" ? "row" : "row-reverse",
				alignItems: "center",
				gap: "0.5em",
				padding: "0.2em 0.6em 0",
				color: ink,
				minWidth: 0,
			}}
		>
			{face}
			<div
				style={{
					display: "flex",
					flexDirection: "column",
					alignItems: side === "l" ? "flex-start" : "flex-end",
					minWidth: 0,
					flex: "1 1 auto",
					paddingBottom: "0.3em",
				}}
			>
				{first ? (
					<span
						style={{
							fontSize: "0.58em",
							fontWeight: 700,
							letterSpacing: "0.08em",
							textTransform: "uppercase",
							opacity: 0.85,
						}}
					>
						{first}
					</span>
				) : null}
				<span
					style={{
						fontSize: "1.05em",
						fontWeight: 900,
						textTransform: "uppercase",
						whiteSpace: "nowrap",
						overflow: "hidden",
						textOverflow: "ellipsis",
						maxWidth: "100%",
					}}
				>
					{last}
				</span>
				<div style={{ display: "flex", gap: "0.35em", marginTop: "0.25em" }}>
					{stat(p.pts, "PTS")}
					{stat(p.trb, "REB")}
					{stat(p.ast, "AST")}
				</div>
			</div>
		</div>
	);
};

// FINAL: the score between the records after the game, and under each team
// its top players' lines.
export const FinalCard = ({
	away,
	home,
	playoffs,
	overtimes,
	narrow,
}: {
	away: RecapCardTeam;
	home: RecapCardTeam;
	playoffs: boolean;
	overtimes: number;
	narrow: boolean;
}) => {
	const panel = usePop(380, 80, 0.85);
	const won = (t: RecapCardTeam, o: RecapCardTeam) => t.pts > o.pts;
	const scoreBox = (t: RecapCardTeam, o: RecapCardTeam) => (
		<div
			style={{
				flex: "1 1 0",
				display: "flex",
				alignItems: "center",
				justifyContent: "center",
				background: "#15161c",
				color: won(t, o) ? "#fff" : "#a9a9b6",
			}}
		>
			<Condensed style={{ fontSize: "2.5em" }}>{t.pts}</Condensed>
		</div>
	);
	const teamBox = (t: RecapCardTeam) => {
		const bg = shade(t.colors[0], -0.15);
		return (
			<div
				style={{
					flex: "1.15 1 0",
					display: "flex",
					flexDirection: "column",
					alignItems: "center",
					justifyContent: "center",
					padding: "0.35em 0.2em",
					background: bg,
					color: inkOn(bg),
				}}
			>
				<Condensed style={{ fontSize: "1.5em" }}>{t.abbrev}</Condensed>
				<span
					style={{
						fontSize: "0.48em",
						fontWeight: 800,
						letterSpacing: "0.12em",
						opacity: 0.85,
						margin: "0.2em 0",
					}}
				>
					{recordLabel(playoffs)}
				</span>
				<Condensed style={{ fontSize: "1em" }}>{t.after}</Condensed>
			</div>
		);
	};
	return (
		<Split away={away} home={home}>
			<div
				style={{
					position: "absolute",
					inset: 0,
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
				}}
			>
				<div
					ref={panel}
					style={{
						width: narrow ? "94cqw" : "64cqw",
						fontSize: narrow
							? "clamp(10px, 3.4cqw, 22px)"
							: "clamp(10px, 2.15cqw, 26px)",
						borderRadius: "0.4em",
						overflow: "hidden",
						boxShadow: "0 0.6em 2.4em rgba(0,0,0,.5)",
					}}
				>
					<div style={{ display: "flex", alignItems: "stretch" }}>
						{teamBox(away)}
						{scoreBox(away, home)}
						<div
							style={{
								flex: "1 1 0",
								display: "flex",
								alignItems: "center",
								justifyContent: "center",
								background: "#050507",
								color: "#fff",
							}}
						>
							<Condensed style={{ fontSize: "1.25em" }}>
								{finalLabel(overtimes)}
							</Condensed>
						</div>
						{scoreBox(home, away)}
						{teamBox(home)}
					</div>
					<div style={{ display: "flex", alignItems: "stretch" }}>
						{[away, home].map((t, i) => (
							<div
								key={i}
								style={{
									flex: "1 1 0",
									minWidth: 0,
									display: "flex",
									flexDirection: "column",
									background: `linear-gradient(180deg, ${shade(t.colors[0], 0.12)}, ${t.colors[0]})`,
									borderTop: `0.2em solid ${t.colors[1]}`,
								}}
							>
								{t.leaders.map((p) => (
									<Leader
										key={p.pid}
										p={p}
										team={t}
										side={i === 0 ? "l" : "r"}
									/>
								))}
							</div>
						))}
					</div>
				</div>
			</div>
		</Split>
	);
};

// A basketball: its leather lit from above, its seams.
const Ball = ({ size, style }: { size: string; style?: CSSProperties }) => {
	// (Safe in a url(): no colons.)
	const id = `ball${useId().replace(/[^\w-]/g, "")}`;
	return (
		<svg
			aria-hidden
			viewBox="-10 -10 20 20"
			style={{ position: "absolute", width: size, height: size, ...style }}
		>
			<defs>
				<radialGradient id={id} cx="36%" cy="30%" r="78%">
					<stop offset="0" stopColor="#f4ab68" />
					<stop offset="0.5" stopColor="#d2661d" />
					<stop offset="1" stopColor="#6e2c0b" />
				</radialGradient>
			</defs>
			<circle r="9.6" fill={`url(#${id})`} />
			<g fill="none" stroke="#2b1206" strokeWidth="0.55">
				<path d="M -9.6 0.4 Q 0 -1.4 9.6 0.4" />
				<path d="M 0.2 -9.6 Q -1.2 0 0.2 9.6" />
				<path d="M -6.6 -7 Q -2.4 0 -6.6 7" />
				<path d="M 6.9 -6.7 Q 2.8 0 6.9 6.7" />
			</g>
		</svg>
	);
};

// A ball dropped in from above the picture, bouncing to rest where it sits.
const useDrop = (on: boolean, delay: number) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		if (!on) {
			return;
		}
		ref.current?.animate?.(
			[
				{ transform: "translateY(-70cqw)", easing: "cubic-bezier(.5,0,1,1)" },
				{
					transform: "translateY(0)",
					offset: 0.55,
					easing: "cubic-bezier(0,0,.5,1)",
				},
				{
					transform: "translateY(-6cqw)",
					offset: 0.78,
					easing: "cubic-bezier(.5,0,1,1)",
				},
				{ transform: "translateY(0)" },
			],
			{ duration: 1100, delay, fill: "backwards" },
		);
	}, [on, delay]);
	return ref;
};

const DroppedBall = ({
	on,
	delay,
	left,
	top,
	size,
}: {
	on: boolean;
	delay: number;
	left: string;
	top: string;
	size: string;
}) => {
	const ref = useDrop(on, delay);
	return (
		<div ref={ref} style={{ position: "absolute", left, top }}>
			<Ball
				size={size}
				style={{
					left: 0,
					top: 0,
					filter: "drop-shadow(0 1cqw 1cqw rgba(0,0,0,.35))",
				}}
			/>
		</div>
	);
};

// How far the floor is tilted away, seen from low down.
const FLOOR_TILT = 62;

// The floor: a half court's lines laid flat, seen from low down - or, as
// the recap opens, from straight overhead, the camera swinging down to it.
const Floor = ({ open }: { open: boolean }) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		if (!open) {
			return;
		}
		ref.current?.animate?.(
			[
				{ transform: "translateY(36cqw) rotateX(0deg)" },
				{ transform: `translateY(0) rotateX(${FLOOR_TILT}deg)` },
			],
			{
				duration: 1700,
				delay: 100,
				easing: "cubic-bezier(.5,0,.2,1)",
				fill: "backwards",
			},
		);
	}, [open]);
	return (
		<div
			aria-hidden
			style={{
				position: "absolute",
				inset: 0,
				perspective: "60cqw",
				perspectiveOrigin: "50% 15%",
				overflow: "hidden",
			}}
		>
			<div
				ref={ref}
				style={{
					position: "absolute",
					left: "50%",
					bottom: "-4cqw",
					width: "104cqw",
					height: "98cqw",
					marginLeft: "-52cqw",
					transformOrigin: "50% 100%",
					transform: `rotateX(${FLOOR_TILT}deg)`,
				}}
			>
				<svg
					viewBox="0 0 500 470"
					style={{ width: "100%", height: "100%", display: "block" }}
				>
					<g fill="none" stroke="#fff" strokeOpacity="0.7" strokeWidth="2.4">
						<rect x="2" y="2" width="496" height="466" />
						<rect x="190" y="2" width="120" height="190" />
						<circle cx="250" cy="192" r="60" />
						<path d="M 30 2 L 30 140 A 237 237 0 0 0 470 140 L 470 2" />
						<path d="M 190 468 A 60 60 0 0 1 310 468" />
						<circle cx="250" cy="52" r="7.5" />
						<path d="M 220 42 L 280 42" />
					</g>
				</svg>
			</div>
		</div>
	);
};

// Up out of the floor, a word at a time.
const useRise = (on: boolean, delay: number) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		if (!on) {
			return;
		}
		ref.current?.animate?.(
			[{ transform: "translateY(105%)" }, { transform: "translateY(0)" }],
			{
				duration: 520,
				delay,
				easing: "cubic-bezier(.2,.8,.2,1.05)",
				fill: "backwards",
			},
		);
	}, [on, delay]);
	return ref;
};

const RisingLine = ({
	on,
	delay,
	children,
}: {
	on: boolean;
	delay: number;
	children: ReactNode;
}) => {
	const ref = useRise(on, delay);
	return (
		<div style={{ overflow: "hidden", padding: "0 0.1em" }}>
			<div ref={ref}>
				<Condensed>{children}</Condensed>
			</div>
		</div>
	);
};

// How long the open runs, before the first game.
export const OPEN_MS = 3400;

// The recap's open, and its poster before it plays (`open` off): its title
// over the blue court, the balls dropped around it, and the night. Opening,
// the camera swings down from over the floor, the balls drop in and the
// title rises out of it.
export const RecapPoster = ({
	when,
	open = false,
	children,
}: {
	when: string;
	open?: boolean;
	children?: ReactNode;
}) => {
	const date = usePop(500, open ? 1700 : 0, 0.9);
	return (
		<div
			style={{
				position: "absolute",
				inset: 0,
				overflow: "hidden",
				fontFamily: FONT,
				background: `radial-gradient(ellipse at 50% 40%, #2f63d8 0, ${DISC} 50%, ${DISC_DARK} 100%)`,
				color: "#fff",
				display: "flex",
				flexDirection: "column",
				alignItems: "center",
				justifyContent: "center",
			}}
		>
			<Floor open={open} />
			<DroppedBall on={open} delay={250} left="9cqw" top="34%" size="9cqw" />
			<DroppedBall on={open} delay={520} left="64cqw" top="12%" size="5cqw" />
			<DroppedBall on={open} delay={900} left="80cqw" top="38%" size="12cqw" />
			<div
				style={{
					position: "relative",
					display: "flex",
					flexDirection: "column",
					alignItems: "center",
					fontSize: "clamp(26px, 9cqw, 110px)",
					textShadow: "0 0.06em 0.2em rgba(0,0,0,.4)",
				}}
			>
				<RisingLine on={open} delay={1000}>
					NIGHTLY
				</RisingLine>
				<RisingLine on={open} delay={1140}>
					RECAP
				</RisingLine>
				<div
					ref={date}
					style={{
						marginTop: "0.5em",
						fontSize: "0.17em",
						fontWeight: 800,
						letterSpacing: "0.2em",
						textTransform: "uppercase",
						opacity: 0.9,
					}}
				>
					{when}
				</div>
			</div>
			{children}
		</div>
	);
};

// How long a stinger runs (see Stinger).
export const STINGER_MS = 1900;

// A face of the stinger's tunnel: the word over and over, running away into
// the picture.
const TunnelFace = ({
	text,
	color,
	face,
}: {
	text: string;
	color: string;
	face: "top" | "bottom" | "left" | "right";
}) => {
	const side = face === "left" || face === "right";
	return (
		<div
			aria-hidden
			style={{
				position: "absolute",
				...(side
					? {
							top: 0,
							[face]: 0,
							width: "160cqw",
							height: "100%",
							transformOrigin: face === "left" ? "0 50%" : "100% 50%",
							transform: `rotateY(${face === "left" ? 90 : -90}deg)`,
							flexDirection: "row",
						}
					: {
							left: 0,
							[face]: 0,
							width: "100%",
							height: "160cqw",
							transformOrigin: face === "top" ? "50% 0" : "50% 100%",
							transform: `rotateX(${face === "top" ? -90 : 90}deg)`,
							flexDirection: "column",
						}),
				display: "flex",
				alignItems: "center",
				justifyContent: "space-around",
				background: "#050507",
				color,
				fontSize: "22cqw",
				overflow: "hidden",
				backfaceVisibility: "hidden",
			}}
		>
			{[0, 1, 2, 3, 4].map((k) => (
				<Condensed
					key={k}
					style={
						side
							? {
									transform: `rotate(${face === "left" ? -90 : 90}deg) scaleX(0.8)`,
								}
							: undefined
					}
				>
					{text}
				</Condensed>
			))}
		</div>
	);
};

// THE STINGER between the halves (and into overtime): the picture gone to
// black, a tunnel of the words in red and blue rushing in, and the words
// themselves in white at its end.
export const Stinger = ({ text }: { text: string }) => {
	// (No fade on the tunnel itself: anything less than opaque flattens it.
	// A veil of black lifts off it instead.)
	const tunnel = useRef<HTMLDivElement | null>(null);
	const veil = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		tunnel.current?.animate?.(
			[
				{ transform: "translateZ(-150cqw)" },
				{ transform: "translateZ(-40cqw)", offset: 0.25 },
				{ transform: "translateZ(0)" },
			],
			{
				duration: STINGER_MS,
				easing: "cubic-bezier(.2,.7,.4,1)",
				fill: "both",
			},
		);
		veil.current?.animate?.([{ opacity: 1 }, { opacity: 0 }], {
			duration: 380,
			fill: "both",
		});
	}, []);
	const title = usePop(420, 380, 1.8);
	const ball = useSlide(-30, 300);
	return (
		<div
			style={{
				position: "absolute",
				inset: 0,
				overflow: "hidden",
				background: "#050507",
				fontFamily: FONT,
				perspective: "50cqw",
			}}
		>
			<div
				ref={tunnel}
				style={{
					position: "absolute",
					inset: 0,
					transformStyle: "preserve-3d",
				}}
			>
				<TunnelFace text={text} color="#2343c9" face="top" />
				<TunnelFace text={text} color="#d4173a" face="bottom" />
				<TunnelFace text={text} color="#9e1230" face="left" />
				<TunnelFace text={text} color="#1a3296" face="right" />
			</div>
			<div
				ref={veil}
				style={{ position: "absolute", inset: 0, background: "#050507" }}
			/>
			<div ref={ball}>
				<Ball size="7cqw" style={{ left: "6cqw", top: "8%" }} />
			</div>
			<div
				ref={title}
				style={{
					position: "absolute",
					inset: 0,
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					color: "#fff",
					fontSize: "13cqw",
					textShadow: "0 0.05em 0.3em rgba(0,0,0,.6)",
				}}
			>
				<Condensed>{text}</Condensed>
			</div>
		</div>
	);
};
