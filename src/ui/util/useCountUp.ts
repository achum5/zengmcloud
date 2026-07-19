import { useEffect, useRef, useState } from "react";

// Animate a number counting up to `target` with an ease-out curve, for stat
// reveals in the trivia games. Restarts whenever `target` changes; `undefined`
// target renders as 0 and does not animate.
export const useCountUp = (
	target: number | undefined,
	durationMs = 650,
): number => {
	const [value, setValue] = useState(0);
	const rafRef = useRef<number | undefined>(undefined);

	useEffect(() => {
		if (rafRef.current !== undefined) {
			cancelAnimationFrame(rafRef.current);
			rafRef.current = undefined;
		}
		if (target === undefined) {
			setValue(0);
			return;
		}
		let startTs: number | undefined;
		const step = (ts: number) => {
			if (startTs === undefined) {
				startTs = ts;
			}
			const t = Math.min(1, (ts - startTs) / durationMs);
			const eased = 1 - (1 - t) ** 3;
			setValue(target * eased);
			if (t < 1) {
				rafRef.current = requestAnimationFrame(step);
			} else {
				rafRef.current = undefined;
			}
		};
		rafRef.current = requestAnimationFrame(step);
		return () => {
			if (rafRef.current !== undefined) {
				cancelAnimationFrame(rafRef.current);
			}
		};
	}, [target, durationMs]);

	return value;
};
