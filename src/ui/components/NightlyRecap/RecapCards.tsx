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
// BOS". Set in a tall condensed face, as broadcast titles are (see
// public/css/recap-fonts.scss), with flat color and square corners. Sized by
// the picture's width (cqw), every size with a floor for a phone.

const FONT = '"Recap Condensed", "Arial Narrow", sans-serif';
const DISC = "#1d3f9e";
const DISC_DARK = "#122a73";

const inkOn = (c: string) => (lightness(c) > 0.62 ? "#101014" : "#fff");

// Tall, heavy type the way the broadcast sets its titles.
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
			fontWeight: 800,
			lineHeight: 0.9,
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
					opacity: 0.12,
					fontSize: "24cqw",
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
				gap: "0.3em",
				padding: "0.5em 0.8em 0.45em",
				minWidth: "4.4em",
				borderRadius: "0.1em",
				background: bg,
				color: inkOn(bg),
				fontSize: "clamp(10px, 1.9cqw, 22px)",
			}}
		>
			<Condensed style={{ fontSize: "2.4em" }}>{team.abbrev}</Condensed>
			{label ? (
				<span
					style={{
						fontSize: "0.66em",
						fontWeight: 700,
						letterSpacing: "0.14em",
						opacity: 0.85,
					}}
				>
					{label}
				</span>
			) : null}
			<Condensed style={{ fontSize: "1.9em" }}>{record}</Condensed>
		</div>
	);
};

// The white dashes around the title's disc, just inside its edge.
const DashedRing = () => (
	<svg
		aria-hidden
		viewBox="0 0 100 100"
		style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }}
	>
		<circle
			cx="50"
			cy="50"
			r="47.5"
			fill="none"
			stroke="#fff"
			strokeWidth="1.8"
			pathLength={200}
			strokeDasharray="7.5 2.5"
		/>
	</svg>
);

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
						position: "relative",
						width: "34cqw",
						height: "34cqw",
						maxHeight: "min(34cqw, 92%)",
						maxWidth: "min(34cqw, 92%)",
						borderRadius: "50%",
						background: `radial-gradient(circle at 50% 35%, ${DISC} 0 55%, ${DISC_DARK} 100%)`,
						boxShadow: "0 1cqw 3cqw rgba(0,0,0,.35)",
						display: "flex",
						flexDirection: "column",
						alignItems: "center",
						justifyContent: "center",
						color: "#fff",
						fontSize: "11cqw",
					}}
				>
					<DashedRing />
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
			<Condensed style={{ fontSize: "1.7em" }}>{n}</Condensed>
			<span
				style={{
					fontSize: "0.52em",
					fontWeight: 700,
					letterSpacing: "0.12em",
					opacity: 0.8,
					marginTop: "0.15em",
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
							fontSize: "0.62em",
							fontWeight: 600,
							letterSpacing: "0.12em",
							textTransform: "uppercase",
							opacity: 0.85,
						}}
					>
						{first}
					</span>
				) : null}
				<span
					style={{
						fontSize: "1.3em",
						fontWeight: 800,
						lineHeight: 1,
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
			<Condensed style={{ fontSize: "2.9em" }}>{t.pts}</Condensed>
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
				<Condensed style={{ fontSize: "1.8em" }}>{t.abbrev}</Condensed>
				<span
					style={{
						fontSize: "0.52em",
						fontWeight: 700,
						letterSpacing: "0.14em",
						opacity: 0.85,
						margin: "0.2em 0",
					}}
				>
					{recordLabel(playoffs)}
				</span>
				<Condensed style={{ fontSize: "1.25em" }}>{t.after}</Condensed>
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
						borderRadius: "0.12em",
						overflow: "hidden",
						boxShadow: "0 0.5em 1.8em rgba(0,0,0,.4)",
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
							<Condensed style={{ fontSize: "1.5em" }}>
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

// How far the floor is tilted away, seen from low down.
const FLOOR_TILT = 62;
// The dark the floor goes off into.
const ARENA = "#040a22";

// The floor: a half court painted blue, its lane a shade darker and lit
// where the title stands, the floor around it going off into the dark -
// seen from low down, or, as the recap opens, from straight overhead, the
// camera swinging down to it. Ten units a foot.
const Floor = ({ open }: { open: boolean }) => {
	// (Safe in a url(): no colons.)
	const id = `floor${useId().replace(/[^\w-]/g, "")}`;
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
					width: "480cqw",
					height: "115cqw",
					marginLeft: "-240cqw",
					transformOrigin: "50% 100%",
					transform: `rotateX(${FLOOR_TILT}deg)`,
				}}
			>
				<svg
					viewBox="-904 -80 2308 550"
					preserveAspectRatio="none"
					style={{ width: "100%", height: "100%", display: "block" }}
				>
					<defs>
						<radialGradient
							id={id}
							gradientUnits="userSpaceOnUse"
							cx="250"
							cy="270"
							r="390"
						>
							<stop offset="0" stopColor="#2b62e3" />
							<stop offset="0.5" stopColor="#1b46bb" />
							<stop offset="1" stopColor="#0d2775" />
						</radialGradient>
						<linearGradient id={`${id}d`} x1="0" y1="0" x2="0" y2="1">
							<stop offset="0" stopColor={ARENA} />
							<stop offset="1" stopColor={ARENA} stopOpacity="0" />
						</linearGradient>
					</defs>
					<rect
						x="-904"
						y="-80"
						width="2308"
						height="550"
						fill={`url(#${id})`}
					/>
					<rect
						x="-904"
						y="-80"
						width="2308"
						height="80"
						fill={`url(#${id}d)`}
					/>
					<rect
						x="170"
						y="2"
						width="160"
						height="190"
						fill="#071d63"
						fillOpacity="0.45"
					/>
					<g fill="none" stroke="#fff" strokeWidth="4.5">
						<rect x="2" y="2" width="496" height="466" />
						<rect x="170" y="2" width="160" height="190" />
						<path d="M 190 192 A 60 60 0 0 0 310 192" />
						<path d="M 190 192 A 60 60 0 0 1 310 192" strokeDasharray="12 10" />
						<path d="M 30 2 L 30 140 A 237.5 237.5 0 0 0 470 140 L 470 2" />
						<path d="M 190 468 A 60 60 0 0 1 310 468" />
						<path d="M 210 54 A 40 40 0 0 0 290 54" strokeWidth="3" />
						<circle cx="250" cy="54" r="7.5" strokeWidth="3" />
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

const RisingWord = ({
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
		<div style={{ overflow: "hidden", padding: "0.04em 0.05em" }}>
			<div ref={ref}>
				<Condensed>{children}</Condensed>
			</div>
		</div>
	);
};

// Faded up into place, once.
const useFadeUp = (on: boolean, delay: number) => {
	const ref = useRef<HTMLDivElement | null>(null);
	useEffect(() => {
		if (!on) {
			return;
		}
		ref.current?.animate?.(
			[
				{ opacity: 0, transform: "translateY(0.6em)" },
				{ opacity: 1, transform: "translateY(0)" },
			],
			{
				duration: 450,
				delay,
				easing: "cubic-bezier(.2,.8,.2,1)",
				fill: "backwards",
			},
		);
	}, [on, delay]);
	return ref;
};

// How long the open runs, before the first game.
export const OPEN_MS = 3400;

// Where the title stands, down the picture: lower in a phone's 4:3, where
// the court's far end comes further down.
export const posterTitleTop = (narrow: boolean) => (narrow ? 0.53 : 0.44);

// The recap's open, and its poster before it plays (`open` off): the title
// on one line over the blue court, the night under it. Opening, the camera
// swings down from over the floor and the title rises out of it.
export const RecapPoster = ({
	when,
	narrow,
	open = false,
	children,
}: {
	when: string;
	narrow: boolean;
	open?: boolean;
	children?: ReactNode;
}) => {
	const date = useFadeUp(open, 1450);
	return (
		<div
			style={{
				position: "absolute",
				inset: 0,
				overflow: "hidden",
				fontFamily: FONT,
				background: ARENA,
				color: "#fff",
			}}
		>
			<Floor open={open} />
			<div
				aria-hidden
				style={{
					position: "absolute",
					inset: 0,
					background:
						"radial-gradient(ellipse 80% 75% at 50% 45%, transparent 55%, rgba(2, 5, 18, 0.55) 100%)",
				}}
			/>
			<div
				style={{
					position: "absolute",
					left: 0,
					right: 0,
					top: `${posterTitleTop(narrow) * 100}%`,
					transform: "translateY(-50%)",
					display: "flex",
					flexDirection: "column",
					alignItems: "center",
				}}
			>
				<div
					style={{
						display: "flex",
						gap: "0.18em",
						fontSize: "clamp(32px, 11.5cqw, 140px)",
						textShadow: "0 0.03em 0.05em rgba(0, 4, 24, 0.35)",
					}}
				>
					<RisingWord on={open} delay={1000}>
						NIGHTLY
					</RisingWord>
					<RisingWord on={open} delay={1120}>
						RECAP
					</RisingWord>
				</div>
				<div
					ref={date}
					style={{
						marginTop: "0.7em",
						paddingLeft: "0.3em",
						fontSize: "clamp(11px, 2.1cqw, 26px)",
						fontWeight: 600,
						letterSpacing: "0.3em",
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

// A face of the stinger's tunnel: the word over and over, a wall of it
// running away into the picture - its rows down the tunnel, each the word
// again and again across the face, every other one set off by half a word.
// (`size`: its height, cqw; `reps`: how many times a row.)
const TUNNEL_ROWS = 6;
const TunnelFace = ({
	text,
	color,
	face,
	size,
	reps,
}: {
	text: string;
	color: string;
	face: "top" | "bottom" | "left" | "right";
	size: number;
	reps: number;
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
						}
					: {
							left: 0,
							[face]: 0,
							width: "100%",
							height: "160cqw",
							transformOrigin: face === "top" ? "50% 0" : "50% 100%",
							transform: `rotateX(${face === "top" ? -90 : 90}deg)`,
						}),
				background: "#050507",
				color,
				fontSize: `${size}cqw`,
				overflow: "hidden",
				backfaceVisibility: "hidden",
			}}
		>
			<div
				style={{
					position: "absolute",
					display: "flex",
					flexDirection: "column",
					alignItems: "center",
					justifyContent: "space-around",
					...(side
						? {
								left: "50%",
								top: "50%",
								width: "160cqw",
								height: "160cqw",
								transform: `translate(-50%, -50%) rotate(${face === "left" ? -90 : 90}deg)`,
							}
						: { inset: 0 }),
				}}
			>
				{Array.from({ length: TUNNEL_ROWS }, (_, row) => (
					<div
						key={row}
						style={{
							display: "flex",
							gap: "0.22em",
							whiteSpace: "nowrap",
							transform: row % 2 === 1 ? "translateX(-0.9em)" : undefined,
						}}
					>
						{Array.from({ length: reps }, (_, k) => (
							<Condensed key={k}>{text}</Condensed>
						))}
					</div>
				))}
			</div>
		</div>
	);
};

// The tunnel of a stinger: its four faces of the word, rushing in from far
// off. (No fade on the tunnel itself: anything less than opaque flattens it.
// A veil of black lifts off it instead.)
const Tunnel = ({
	text,
	colors,
	ms,
}: {
	text: string;
	// Top, bottom, left, right.
	colors: [string, string, string, string];
	ms: number;
}) => {
	const tunnel = useRef<HTMLDivElement | null>(null);
	const veil = useRef<HTMLDivElement | null>(null);
	// (A condensed capital is about 0.37 of its height across: a long title
	// smaller, most of it across a face.)
	const across = 0.37 * text.length;
	const size = Math.min(22, 110 / across);
	const fit = {
		size,
		reps: Math.max(1, Math.ceil(130 / (size * (across + 0.22))) + 1),
	};
	useEffect(() => {
		tunnel.current?.animate?.(
			[
				{ transform: "translateZ(-150cqw)" },
				{ transform: "translateZ(-40cqw)", offset: 0.25 },
				{ transform: "translateZ(0)" },
			],
			{
				duration: ms,
				easing: "cubic-bezier(.2,.7,.4,1)",
				fill: "both",
			},
		);
		veil.current?.animate?.([{ opacity: 1 }, { opacity: 0 }], {
			duration: 380,
			fill: "both",
		});
	}, [ms]);
	return (
		<>
			<div
				aria-hidden
				style={{
					position: "absolute",
					inset: 0,
					perspective: "50cqw",
					zIndex: 0,
					overflow: "hidden",
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
					<TunnelFace text={text} color={colors[0]} face="top" {...fit} />
					<TunnelFace text={text} color={colors[1]} face="bottom" {...fit} />
					<TunnelFace text={text} color={colors[2]} face="left" {...fit} />
					<TunnelFace text={text} color={colors[3]} face="right" {...fit} />
				</div>
			</div>
			<div
				ref={veil}
				style={{
					position: "absolute",
					inset: 0,
					zIndex: 0,
					background: "#050507",
				}}
			/>
		</>
	);
};

const RED_BLUE: [string, string, string, string] = [
	"#2343c9",
	"#d4173a",
	"#9e1230",
	"#1a3296",
];

// THE STINGER between the halves (and into overtime): the picture gone to
// black, a tunnel of the words in red and blue rushing in, and the words
// themselves in white at its end.
export const Stinger = ({ text }: { text: string }) => {
	const pop = usePop(420, 380, 1.8);
	return (
		<div
			style={{
				position: "absolute",
				inset: 0,
				overflow: "hidden",
				background: "#050507",
				fontFamily: FONT,
			}}
		>
			<Tunnel text={text} colors={RED_BLUE} ms={STINGER_MS} />
			<div
				ref={pop}
				style={{
					position: "absolute",
					inset: 0,
					display: "flex",
					alignItems: "center",
					justifyContent: "center",
					color: "#fff",
					fontSize: "16cqw",
					textShadow: "0 0.04em 0.2em rgba(0,0,0,.6)",
				}}
			>
				<Condensed>{text}</Condensed>
			</div>
		</div>
	);
};
