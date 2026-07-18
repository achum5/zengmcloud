import SelectMultiple from "./SelectMultiple/index.tsx";

// The guess input shared by the trivia games: a virtualized searchable list
// of every player in league history. Selection resets after every pick so it
// can be used for guess-after-guess without clearing manually.

export type TriviaSearchPlayer = {
	pid: number;
	name: string;
	years: string;
	pos?: string;
	teams?: string;
};

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
}) => (
	<SelectMultiple<TriviaSearchPlayer>
		value={null}
		options={players}
		onChange={(p) => {
			if (p) {
				onSelect(p);
			}
		}}
		getOptionLabel={(p) => {
			const extra = [p.pos, p.teams].filter(Boolean).join(" · ");
			return `${p.name} (${p.years})${extra ? ` — ${extra}` : ""}`;
		}}
		getOptionValue={(p) => String(p.pid)}
		isClearable={false}
		disabled={disabled}
		autoFocus={autoFocus}
		placeholder="Search players…"
	/>
);

export default TriviaPlayerSelect;
