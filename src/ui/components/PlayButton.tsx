import clsx from "clsx";

// A small round play button - for watching a game's replay, or a player's
// highlights from it. The triangle is drawn, not a font glyph, so it shows
// on every device.
export const PlayButton = ({
	className,
	disabled,
	onClick,
	title,
}: {
	className?: string;
	disabled?: boolean;
	onClick: () => void;
	title: string;
}) => {
	return (
		<button
			type="button"
			className={clsx(
				"btn btn-secondary btn-sm rounded-circle p-0 d-inline-flex align-items-center justify-content-center flex-shrink-0",
				className,
			)}
			style={{ width: 22, height: 22, verticalAlign: "middle" }}
			onClick={onClick}
			disabled={disabled}
			title={title}
			aria-label={title}
		>
			<svg width="9" height="10" viewBox="0 0 9 10" aria-hidden="true">
				<path d="M1 0.8 L8.4 5 L1 9.2 Z" fill="currentColor" />
			</svg>
		</button>
	);
};

export default PlayButton;
