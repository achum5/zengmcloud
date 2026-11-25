import {
	useCallback,
	useEffect,
	useState,
	type CSSProperties,
	type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { usePlayerFace } from "../util/playerFaces.ts";
import { PlayerPicture } from "./PlayerPicture.tsx";
import type {
	ReviewPlayer,
	ReviewTeam,
	SeasonReview as Review,
} from "../../worker/util/seasonReview.ts";

// THE SEASON IN REVIEW: a finished season a slide at a time, tapped through
// like a story - the champions, the awards, the leaders, your season, and
// who retired. Opened from the Season Summary.

const Face = ({
	pid,
	season,
	className,
}: {
	pid: number;
	season: number;
	className: string;
}) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	return (
		<div className={className}>
			{face && (face.face || face.imgURL) ? (
				<PlayerPicture
					face={face.face}
					imgURL={face.imgURL}
					colors={face.colors}
					jersey={face.jersey}
				/>
			) : null}
		</div>
	);
};

const Logo = ({
	team,
	className,
}: {
	team: { imgURL: string | undefined };
	className: string;
}) =>
	team.imgURL ? <img className={className} src={team.imgURL} alt="" /> : null;

const record = (t: ReviewTeam) =>
	helpers.formatRecord({ won: t.won, lost: t.lost, tied: t.tied, otl: t.otl });

type Slide = {
	key: string;
	colors?: [string, string, string];
	body: ReactNode;
};

// The darker of a team's first two colors behind the slide, the other as its
// stripe; a light one is darkened so white type still reads on it.
const luminance = (hex: string) => {
	const m = /^#?([\da-f]{6})$/i.exec(hex.trim());
	if (!m) {
		return 0;
	}
	const n = Number.parseInt(m[1]!, 16);
	return (
		(0.2126 * ((n >> 16) & 255) +
			0.7152 * ((n >> 8) & 255) +
			0.0722 * (n & 255)) /
		255
	);
};
const slideStyle = (colors: Slide["colors"]): CSSProperties | undefined => {
	if (!colors) {
		return;
	}
	const [a, b, c] = colors;
	const [bg, other] = luminance(a) <= luminance(b) ? [a, b] : [b, a];
	// (Not a white stripe, when the team has a third color to use.)
	const stripe = luminance(other) > 0.9 ? c : other;
	return {
		"--sr-bg":
			luminance(bg) > 0.45 ? `color-mix(in srgb, ${bg} 45%, #000)` : bg,
		"--sr-stripe": stripe,
	} as CSSProperties;
};

const Stats = ({ p }: { p: ReviewPlayer }) => (
	<div className="sr-stats">
		{p.stats.map((stat) => (
			<div key={stat.label}>
				<b>{stat.value}</b>
				<span>{stat.label}</span>
			</div>
		))}
	</div>
);

const buildSlides = (review: Review, onClose: () => void) => {
	const { season } = review;
	const slides: Slide[] = [];
	const champ = review.champion;

	slides.push({
		key: "cover",
		colors: champ?.colors,
		body: (
			<div className="sr-center">
				{champ ? <Logo team={champ} className="sr-cover-logo" /> : null}
				<div className="sr-kicker">Season in review</div>
				<div className="sr-huge">{season}</div>
				<div className="sr-hint">Tap to start</div>
			</div>
		),
	});

	if (champ) {
		slides.push({
			key: "champ",
			colors: champ.colors,
			body: (
				<div className="sr-center">
					<div className="sr-kicker">Champions</div>
					<Logo team={champ} className="sr-big-logo" />
					<div className="sr-title">
						{champ.region} {champ.name}
					</div>
					<div className="sr-line">{record(champ)}</div>
					{champ.finals ? (
						<div className="sr-line">
							Beat {champ.finals.opp.region} {champ.finals.won}-
							{champ.finals.lost} in the finals
						</div>
					) : null}
				</div>
			),
		});
	}

	const playerSlide = (
		key: string,
		kicker: string,
		p: ReviewPlayer,
		note?: string,
	): Slide => ({
		key,
		colors: p.colors ?? champ?.colors,
		body: (
			<div className="sr-center">
				<div className="sr-kicker">{kicker}</div>
				<Face pid={p.pid} season={season} className="sr-face" />
				<div className="sr-title">
					<a href={helpers.leagueUrl(["player", p.pid])}>{p.name}</a>
				</div>
				<div className="sr-line">{p.abbrev}</div>
				<Stats p={p} />
				{note ? <div className="sr-note">{note}</div> : null}
			</div>
		),
	});

	// The MVPs get a slide of their own, finals first; the rest four at a time.
	const big = review.awards.filter((award) => award.big);
	const regular = big.filter((award) => award.note === "Per game");
	for (const award of [
		...big.filter((award) => !regular.includes(award)),
		...regular,
	]) {
		slides.push(playerSlide(award.name, award.name, award.player, award.note));
	}
	const rest = review.awards.filter((award) => !big.includes(award));
	for (let i = 0; i < rest.length; i += 4) {
		const group = rest.slice(i, i + 4);
		slides.push({
			key: `awards${i}`,
			colors: champ?.colors,
			body: (
				<div className="sr-top">
					<div className="sr-kicker">The awards</div>
					<div className="sr-grid">
						{group.map((award) => (
							<div key={award.name} className="sr-cell">
								<Face
									pid={award.player.pid}
									season={season}
									className="sr-face-sm"
								/>
								<div className="sr-cell-award">{award.name}</div>
								<div className="sr-cell-name">
									<a href={helpers.leagueUrl(["player", award.player.pid])}>
										{award.player.name}
									</a>
								</div>
								<div className="sr-cell-team">{award.player.abbrev}</div>
							</div>
						))}
					</div>
				</div>
			),
		});
	}

	if (review.leaders.length > 0) {
		slides.push({
			key: "leaders",
			colors: champ?.colors,
			body: (
				<div className="sr-top">
					<div className="sr-kicker">League leaders</div>
					<div className="sr-rows">
						{review.leaders.map((row) => (
							<div key={row.label} className="sr-row">
								<Face
									pid={row.player.pid}
									season={season}
									className="sr-face-row"
								/>
								<div className="sr-row-main">
									<div className="sr-row-label">{row.label}</div>
									<div className="sr-row-name">
										<a href={helpers.leagueUrl(["player", row.player.pid])}>
											{row.player.name}
										</a>{" "}
										<span>{row.player.abbrev}</span>
									</div>
								</div>
								<div className="sr-row-value">
									<b>{row.value}</b>
									<span>per game</span>
								</div>
							</div>
						))}
					</div>
				</div>
			),
		});
	}

	const user = review.user;
	if (user) {
		slides.push({
			key: "user",
			colors: user.colors,
			body: (
				<div className="sr-center">
					<div className="sr-kicker">Your season</div>
					<Logo team={user} className="sr-big-logo" />
					<div className="sr-title">
						{user.region} {user.name}
					</div>
					<div className="sr-huge sr-record">{record(user)}</div>
					<div className="sr-line">{user.result}</div>
					{user.best ? (
						<div className="sr-row sr-best">
							<Face
								pid={user.best.pid}
								season={season}
								className="sr-face-row"
							/>
							<div className="sr-row-main">
								<div className="sr-row-label">Best player</div>
								<div className="sr-row-name">
									<a href={helpers.leagueUrl(["player", user.best.pid])}>
										{user.best.name}
									</a>
								</div>
							</div>
							<div className="sr-row-value">
								<b>{user.best.ws.toFixed(1)}</b>
								<span>WS</span>
							</div>
						</div>
					) : null}
				</div>
			),
		});
	}

	if (review.retired.length > 0) {
		slides.push({
			key: "retired",
			colors: review.retired[0]!.colors ?? champ?.colors,
			body: (
				<div className="sr-top">
					<div className="sr-kicker">Farewell</div>
					<div className="sr-rows">
						{review.retired.map((p) => (
							<div key={p.pid} className="sr-row">
								<Face pid={p.pid} season={season} className="sr-face-row" />
								<div className="sr-row-main">
									<div className="sr-row-name">
										<a href={helpers.leagueUrl(["player", p.pid])}>{p.name}</a>
									</div>
									<div className="sr-row-label">
										{p.from}-{p.to} · {p.stats[0]?.value} seasons
									</div>
								</div>
								<div className="sr-row-value">
									<b>{p.ws.toFixed(1)}</b>
									<span>career WS</span>
								</div>
							</div>
						))}
					</div>
				</div>
			),
		});
	}

	slides.push({
		key: "end",
		colors: champ?.colors,
		body: (
			<div className="sr-center">
				<div className="sr-kicker">That was</div>
				<div className="sr-huge">{season}</div>
				<button type="button" className="sr-end-btn" onClick={onClose}>
					Done
				</button>
			</div>
		),
	});

	return slides;
};

export const SeasonReview = ({
	season,
	onClose,
}: {
	season: number;
	onClose: () => void;
}) => {
	const [review, setReview] = useState<Review | undefined | null>(null);
	const [index, setIndex] = useState(0);

	useEffect(() => {
		let cancelled = false;
		(async () => {
			const value = await toWorker("main", "getSeasonReview", season);
			if (!cancelled) {
				setReview(value);
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [season]);

	const slides = review ? buildSlides(review, onClose) : [];
	const count = slides.length;

	const go = useCallback(
		(step: number) => {
			const next = index + step;
			if (next < count) {
				setIndex(Math.max(0, next));
			} else if (count > 0) {
				// Past the last slide, the story is over.
				onClose();
			}
		},
		[count, index, onClose],
	);

	useEffect(() => {
		const onKey = (event: KeyboardEvent) => {
			if (event.key === "Escape") {
				onClose();
			} else if (event.key === "ArrowRight" || event.key === " ") {
				event.preventDefault();
				go(1);
			} else if (event.key === "ArrowLeft") {
				event.preventDefault();
				go(-1);
			}
		};
		document.addEventListener("keydown", onKey);
		return () => {
			document.removeEventListener("keydown", onKey);
		};
	}, [go, onClose]);

	// The page behind stays put while the story is up.
	useEffect(() => {
		const overflow = document.body.style.overflow;
		document.body.style.overflow = "hidden";
		return () => {
			document.body.style.overflow = overflow;
		};
	}, []);

	const slide = slides[Math.min(index, count - 1)];
	const colors = slide?.colors;

	return createPortal(
		// eslint-disable-next-line jsx-a11y/click-events-have-key-events, jsx-a11y/no-static-element-interactions -- The keys (arrows, space, Esc) are listened for on the document
		<div className="sr-backdrop" onClick={onClose}>
			{/* eslint-disable-next-line jsx-a11y/click-events-have-key-events, jsx-a11y/no-static-element-interactions -- As above */}
			<div
				className="sr-story"
				style={slideStyle(colors)}
				onClick={(event) => {
					event.stopPropagation();
					if ((event.target as HTMLElement).closest("a, button")) {
						return;
					}
					const box = event.currentTarget.getBoundingClientRect();
					go(event.clientX - box.left < box.width * 0.3 ? -1 : 1);
				}}
			>
				<div className="sr-bars">
					{slides.map((s, i) => (
						<span
							key={s.key}
							className={i <= index ? "sr-bar sr-bar-on" : "sr-bar"}
						/>
					))}
				</div>
				<button
					type="button"
					className="sr-close"
					aria-label="Close"
					onClick={onClose}
				>
					×
				</button>
				<div className="sr-slide" key={slide?.key}>
					{review === null ? (
						<div className="sr-center sr-hint">Loading…</div>
					) : review === undefined ? (
						<div className="sr-center sr-hint">Nothing to review yet.</div>
					) : (
						slide?.body
					)}
				</div>
			</div>
		</div>,
		document.body,
	);
};

export const SeasonReviewButton = ({ season }: { season: number }) => {
	const [open, setOpen] = useState(false);
	if (__SPORT !== "basketball") {
		return null;
	}
	return (
		<>
			<button
				type="button"
				className="btn btn-primary mb-3"
				onClick={() => setOpen(true)}
			>
				▶ Season in review
			</button>
			{open ? (
				<SeasonReview season={season} onClose={() => setOpen(false)} />
			) : null}
		</>
	);
};
