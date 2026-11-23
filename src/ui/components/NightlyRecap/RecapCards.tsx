import { useEffect, useRef, type CSSProperties, type ReactNode } from "react";
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
				<Condensed
					style={{ color: inkOn(team.colors[0]), fontSize: "11cqw" }}
				>
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
	const face = (
		<div
			style={{
				width: "3.4em",
				height: "3.4em",
				flex: "0 0 auto",
				alignSelf: "flex-end",
				overflow: "hidden",
			}}
		>
			<PlayerPicture
				face={p.face}
				imgURL={p.imgURL}
				colors={team.colors}
				jersey={undefined}
			/>
		</div>
	);
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

// The recap before it plays: its title over the league's blue court, and
// the night.
export const RecapPoster = ({
	when,
	children,
}: {
	when: string;
	children?: ReactNode;
}) => (
	<div
		style={{
			position: "absolute",
			inset: 0,
			overflow: "hidden",
			fontFamily: FONT,
			background: `radial-gradient(ellipse at 50% 60%, #2f63d8 0, ${DISC} 55%, ${DISC_DARK} 100%)`,
			color: "#fff",
			display: "flex",
			flexDirection: "column",
			alignItems: "center",
			justifyContent: "center",
		}}
	>
		<CourtLines />
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
			<Condensed>NIGHTLY</Condensed>
			<Condensed>RECAP</Condensed>
			<span
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
			</span>
		</div>
		{children}
	</div>
);

// A half court's lines in white, in perspective under the title.
const CourtLines = () => (
	<svg
		aria-hidden
		viewBox="0 0 160 90"
		preserveAspectRatio="xMidYMid slice"
		style={{
			position: "absolute",
			inset: 0,
			width: "100%",
			height: "100%",
			opacity: 0.55,
		}}
	>
		<g
			fill="none"
			stroke="#fff"
			strokeWidth="0.7"
			transform="translate(80 58) scale(1 0.42) translate(-80 -58)"
		>
			<rect x="10" y="-30" width="140" height="150" />
			<rect x="62" y="-30" width="36" height="58" />
			<circle cx="80" cy="28" r="18" />
			<path d="M 18 -30 L 18 18 A 64 64 0 0 0 142 18 L 142 -30" />
			<circle cx="80" cy="120" r="18" />
		</g>
	</svg>
);
