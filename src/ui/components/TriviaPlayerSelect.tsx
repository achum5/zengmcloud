import {
	type KeyboardEvent,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import { PlayerPicture } from "./PlayerPicture.tsx";
import {
	fetchTriviaCard,
	getCachedCard,
	type TriviaPlayerCard,
} from "../util/triviaPlayerCards.ts";

// The guess input shared by the trivia games.
//
// This is a purpose-built combobox rather than the generic SelectMultiple,
// for two reasons. First, results show the player's FACE, and the shared
// select virtualizes on a fixed 32px row height that a face doesn't fit.
// Second, and more importantly, the result rows must not leak the answer:
// listing the teams a player suited up for hands you every team cell on the
// board for free, which is most of the game.

export type TriviaSearchPlayer = {
	pid: number;
	name: string;
	years: string;
	pos?: string;
};

const MAX_RESULTS = 8;

// Diacritic-insensitive, so "jokic" finds "Jokić".
const normalize = (s: string) =>
	s
		.normalize("NFD")
		.replace(/[̀-ͯ]/g, "")
		.toLowerCase();

// Rank: a word starting with the query beats a mid-word hit, and a hit on the
// last word (the surname, usually) beats one on a first name - typing "james"
// should surface LeBron James above Jamesons.
const scoreMatch = (name: string, q: string): number => {
	const n = normalize(name);
	if (!n.includes(q)) {
		return -1;
	}
	const words = n.split(" ");
	const lastWord = words.at(-1) ?? "";
	if (lastWord.startsWith(q)) {
		return 0;
	}
	if (words.some((w) => w.startsWith(q))) {
		return 1;
	}
	return 2;
};

const Face = ({ card }: { card: TriviaPlayerCard | undefined }) => (
	<div
		className="flex-shrink-0 overflow-hidden"
		style={{ width: 30, height: 40 }}
	>
		{card ? (
			<PlayerPicture
				face={card.face}
				imgURL={card.imgURL}
				colors={card.colors}
				jersey={card.jersey}
				lazy
			/>
		) : null}
	</div>
);

const TriviaPlayerSelect = ({
	players,
	onSelect,
	disabled,
	autoFocus,
}: {
	players: TriviaSearchPlayer[];
	onSelect: (player: TriviaSearchPlayer) => void;
	disabled?: boolean;
	autoFocus?: boolean;
}) => {
	const [query, setQuery] = useState("");
	const [highlighted, setHighlighted] = useState(0);
	const [cards, setCards] = useState<Record<number, TriviaPlayerCard>>({});
	const inputRef = useRef<HTMLInputElement>(null);

	useEffect(() => {
		if (autoFocus) {
			inputRef.current?.focus();
		}
	}, [autoFocus]);

	const results = useMemo(() => {
		const q = normalize(query.trim());
		if (q.length < 2) {
			return [];
		}
		const scored: { p: TriviaSearchPlayer; score: number }[] = [];
		for (const p of players) {
			const score = scoreMatch(p.name, q);
			if (score >= 0) {
				scored.push({ p, score });
				// Bail once there is plenty to rank - a two-letter query in a deep
				// league would otherwise score every player on every keystroke.
				if (scored.length > 400) {
					break;
				}
			}
		}
		scored.sort(
			(a, b) => a.score - b.score || a.p.name.localeCompare(b.p.name),
		);
		return scored.slice(0, MAX_RESULTS).map((s) => s.p);
	}, [players, query]);

	// Keep the highlight in range as the result set changes under it.
	useEffect(() => {
		setHighlighted(0);
	}, [query]);

	// Faces for whatever is on screen. Cached across queries, so backspacing
	// through a search doesn't re-fetch anything.
	useEffect(() => {
		let stale = false;
		for (const p of results) {
			const cached = getCachedCard(p.pid);
			if (cached) {
				setCards((prev) => (prev[p.pid] ? prev : { ...prev, [p.pid]: cached }));
				continue;
			}
			void fetchTriviaCard(p.pid).then((card) => {
				if (card && !stale) {
					setCards((prev) => (prev[p.pid] ? prev : { ...prev, [p.pid]: card }));
				}
			});
		}
		return () => {
			stale = true;
		};
	}, [results]);

	const pick = (p: TriviaSearchPlayer | undefined) => {
		if (!p) {
			return;
		}
		setQuery("");
		setHighlighted(0);
		onSelect(p);
		inputRef.current?.focus();
	};

	const onKeyDown = (e: KeyboardEvent) => {
		if (e.key === "ArrowDown") {
			e.preventDefault();
			setHighlighted((h) => Math.min(results.length - 1, h + 1));
		} else if (e.key === "ArrowUp") {
			e.preventDefault();
			setHighlighted((h) => Math.max(0, h - 1));
		} else if (e.key === "Enter") {
			e.preventDefault();
			pick(results[highlighted]);
		} else if (e.key === "Escape") {
			setQuery("");
		}
	};

	const showEmpty = query.trim().length >= 2 && results.length === 0;

	return (
		<div>
			<input
				ref={inputRef}
				className="form-control"
				type="text"
				value={query}
				disabled={disabled}
				placeholder="Search players…"
				autoComplete="off"
				spellCheck={false}
				onChange={(e) => setQuery(e.target.value)}
				onKeyDown={onKeyDown}
			/>
			{results.length > 0 ? (
				<div className="trivia-search-results mt-1">
					{results.map((p, i) => (
						<button
							key={p.pid}
							type="button"
							className={`trivia-search-row ${
								i === highlighted ? "is-highlighted" : ""
							}`}
							onMouseEnter={() => setHighlighted(i)}
							onClick={() => pick(p)}
						>
							<Face card={cards[p.pid]} />
							<span className="flex-grow-1 text-start text-truncate">
								<span className="d-block text-truncate">{p.name}</span>
								<span className="d-block small text-body-secondary">
									{p.pos ? `${p.pos} · ` : ""}
									{p.years}
								</span>
							</span>
						</button>
					))}
				</div>
			) : null}
			{showEmpty ? (
				<div className="text-body-secondary small mt-2">
					No player matches “{query.trim()}”.
				</div>
			) : null}
		</div>
	);
};

export default TriviaPlayerSelect;
