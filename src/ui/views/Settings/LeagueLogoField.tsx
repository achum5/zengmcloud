import { useState } from "react";
import ImageUploader from "../../components/ImageUploader.tsx";
import { DEFAULT_LEAGUE_LOGO } from "../../components/NewInterface/leagueLogo.ts";

// The league's logo for the new interface: a link, or an uploaded image (which
// becomes a link). Blank means the game's own logo.
const LeagueLogoField = ({
	disabled,
	id,
	onChange,
	value,
}: {
	disabled: boolean;
	id: string;
	onChange: (value: string) => void;
	value: string;
}) => {
	const [showUpload, setShowUpload] = useState(false);

	return (
		<div className="d-flex flex-column gap-2">
			<div className="d-flex gap-2 align-items-center">
				<img
					alt=""
					src={value || DEFAULT_LEAGUE_LOGO}
					width={32}
					height={32}
					style={{ objectFit: "contain", flex: "none" }}
				/>
				<input
					id={id}
					type="text"
					className="form-control"
					disabled={disabled}
					placeholder="Image URL"
					value={value}
					onChange={(event) => onChange(event.target.value)}
				/>
				<button
					type="button"
					className="btn btn-secondary"
					disabled={disabled}
					onClick={() => setShowUpload((show) => !show)}
				>
					Upload
				</button>
			</div>
			{showUpload ? (
				<ImageUploader
					disabled={disabled}
					onUploaded={(url) => {
						onChange(url);
						setShowUpload(false);
					}}
				/>
			) : null}
		</div>
	);
};

export default LeagueLogoField;
