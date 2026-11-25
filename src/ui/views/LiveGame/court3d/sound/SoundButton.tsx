import { sleepSound, wakeSound } from "./context.ts";
import { setSound, useSound } from "./setting.ts";

// The speaker among the live game's controls: game sound on or off on this
// device (the same as Global Settings' "Game Sound"). The tap that turns it
// on is what lets the page make sound at all.
const SoundButton = ({ className }: { className: string }) => {
	const on = useSound();
	return (
		<button
			type="button"
			className={className}
			title={on ? "Sound off" : "Sound on"}
			aria-pressed={on}
			onClick={() => {
				if (on) {
					setSound(false);
					sleepSound();
				} else {
					wakeSound();
					setSound(true);
				}
			}}
		>
			<svg
				width="16"
				height="16"
				viewBox="0 0 16 16"
				fill="currentColor"
				aria-hidden="true"
				style={{ verticalAlign: "-0.15em" }}
			>
				<path d="M2 6h2.5L8 3v10L4.5 10H2z" />
				{on ? (
					<path
						d="M10.5 5.2a4 4 0 0 1 0 5.6M12.4 3.4a6.5 6.5 0 0 1 0 9.2"
						fill="none"
						stroke="currentColor"
						strokeWidth="1.4"
						strokeLinecap="round"
					/>
				) : (
					<path
						d="M10.5 6l4 4M14.5 6l-4 4"
						fill="none"
						stroke="currentColor"
						strokeWidth="1.4"
						strokeLinecap="round"
					/>
				)}
			</svg>
		</button>
	);
};

export default SoundButton;
