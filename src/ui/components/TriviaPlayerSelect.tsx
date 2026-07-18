import SelectMultiple from "./SelectMultiple/index.tsx";

// The guess input shared by the trivia games: a virtualized searchable list
// of every player in league history. Selection resets after every pick so it
// can be used for guess-after-guess without clearing manually.

export type TriviaSearchPlayer = {
	pid: number;
	name: string;
	years: string;
};

const TriviaPlayerSelect = ({
	players,
	onSelect,
	disabled,
}: {
	players: TriviaSearchPlayer[];
	onSelect: (player: TriviaSearchPlayer) => void;
	disabled?: boolean;
}) => (
	<SelectMultiple<TriviaSearchPlayer>
		value={null}
		options={players}
		onChange={(p) => {
			if (p) {
				onSelect(p);
			}
		}}
		getOptionLabel={(p) => `${p.name} (${p.years})`}
		getOptionValue={(p) => String(p.pid)}
		isClearable={false}
		disabled={disabled}
	/>
);

export default TriviaPlayerSelect;
