import clsx from "clsx";
import { useId } from "react";
import { allCityNames, findCity } from "../../../common/teamLocation.ts";

const LIST_ID = "manage-teams-home-cities";

// Every city a team can be put in, once per page.
export const HomeCityList = () => (
	<datalist id={LIST_ID}>
		{allCityNames().map((city) => (
			<option key={city} value={city} />
		))}
	</datalist>
);

// Where the team plays, for travel. Blank means its region's city; a region
// that isn't on the map needs one.
export const HomeCityInput = ({
	region,
	value,
	onChange,
}: {
	region: string;
	value: string;
	onChange: (value: string) => void;
}) => {
	const id = useId();
	const regionCity = findCity(region);
	const unknown =
		value.trim() === "" ? regionCity === undefined : !findCity(value);

	return (
		<div className="input-group input-group-sm" style={{ maxWidth: 260 }}>
			<label
				className="input-group-text"
				htmlFor={id}
				title="Where the team plays, for travel"
			>
				Home city
			</label>
			<input
				id={id}
				className={clsx("form-control", unknown && "is-invalid")}
				list={LIST_ID}
				placeholder={regionCity ?? "Not on the map"}
				value={value}
				onChange={(event) => {
					onChange(event.target.value);
				}}
			/>
		</div>
	);
};
