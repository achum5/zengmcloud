// WHERE THE PINNED BARS ACTUALLY PAINT, MEASURED ON THE GLASS.
//
// Six attempts at the iOS sticky fault have now failed, and the reason is that
// every one of them had to trust a viewport number to decide where the top and
// bottom of the screen are. The latest field report proves those numbers cannot
// be trusted in EITHER direction at once: the device reports a visual viewport
// 646 CSS px tall inside a 1052 layout viewport, with the two IDENTICAL
// horizontally (518 and 518). A zoom shrinks both axes together - measured, in
// a browser, 440x956 becomes 270x587 at 1.63x - so a shortfall on one axis only
// is not a zoom. It is a ~406px inset the device is reporting with nothing
// focused, and it leaves the two candidate anchors disagreeing by exactly that:
// documentElement.clientHeight puts the ticker below the glass, and
// visualViewport.offsetTop + height puts it in the middle of the page. Both
// have now been shipped and both were reported wrong.
//
// A touch event settles it without asking the viewport anything. Every touch
// carries clientY - the coordinate system our anchors and getBoundingClientRect
// speak - alongside screenY, the position on the physical screen. Two taps far
// enough apart therefore give the whole mapping between the two:
//
//     screenY = originY + scale * clientY
//
// originY is then the physical position of client y = 0, which is exactly where
// a `top: 0` pinned header claims to be, and scale is the real scale rather
// than the reported one. Feed the bars' own measured rects through it and the
// answer is where they land on the glass - the one question no report so far
// has been able to answer, and the one the user has been answering by eye.
//
// Nothing here instructs the user or shows any UI. Ordinary taps are the
// samples, so by the time the report button is pressed there are dozens.

export type TouchSample = {
	// The tap in the page's own coordinate system.
	clientY: number;
	// The same tap on the physical screen.
	screenY: number;
	// visualViewport.offsetTop when it was taken. Panning within a zoomed page
	// moves the mapping, so samples from different offsets cannot be mixed.
	offsetTop: number;
};

export type TouchMapping = {
	// Physical screen position of client y = 0: where a bar pinned to the top
	// of the viewport actually paints.
	originY: number;
	// Physical points per CSS pixel, measured rather than reported.
	scale: number;
	// How far apart the samples were in client space. A fit over a narrow
	// spread cannot separate the origin from the scale, so this is reported
	// alongside the answer rather than hidden inside it.
	spread: number;
	samples: number;
};

// Below this, two taps are close enough that any error in either one swings
// the fitted scale wildly - and the scale multiplies through to every
// conclusion drawn from it.
const MIN_SPREAD = 8;

// Fit screenY = originY + scale * clientY by least squares over the samples
// that share the newest visual-viewport offset. Undefined when the samples
// cannot support an answer, which is honest and is the whole point: a made-up
// mapping here would be the seventh wrong anchor.
export const solveTouchMapping = (
	samples: readonly TouchSample[],
): TouchMapping | undefined => {
	if (samples.length < 2) {
		return undefined;
	}

	// Panning a zoomed page slides the client-to-screen mapping, so only
	// samples taken at the same offset describe one mapping. The newest offset
	// is the one the user is looking at now.
	const offsetTop = samples.at(-1)!.offsetTop;
	const usable = samples.filter(
		(sample) =>
			sample.offsetTop === offsetTop &&
			Number.isFinite(sample.clientY) &&
			Number.isFinite(sample.screenY),
	);
	if (usable.length < 2) {
		return undefined;
	}

	const ys = usable.map((sample) => sample.clientY);
	const spread = Math.max(...ys) - Math.min(...ys);
	if (spread < MIN_SPREAD) {
		return undefined;
	}

	const n = usable.length;
	const meanClient = ys.reduce((a, b) => a + b, 0) / n;
	const meanScreen =
		usable.reduce((total, sample) => total + sample.screenY, 0) / n;
	let covariance = 0;
	let variance = 0;
	for (const sample of usable) {
		const d = sample.clientY - meanClient;
		covariance += d * (sample.screenY - meanScreen);
		variance += d * d;
	}
	if (variance === 0) {
		return undefined;
	}

	const scale = covariance / variance;
	return {
		originY: round(meanScreen - scale * meanClient),
		scale: Math.round(scale * 1000) / 1000,
		spread: round(spread),
		samples: n,
	};
};

// Where an element measured in client coordinates actually lands on the glass.
// The whole point of the mapping: rect.top of 0 does NOT mean "at the top of
// the screen" on the device this exists for.
export const projectToScreen = (
	mapping: TouchMapping,
	clientY: number,
): number => round(mapping.originY + mapping.scale * clientY);

// Is a bar spanning these physical positions actually on the glass? The
// verdict the user has been giving by eye, stated by the app for once.
export const screenVerdict = ({
	top,
	bottom,
	screenHeight,
}: {
	top: number;
	bottom: number;
	screenHeight: number;
}): "visible" | "above" | "below" | "clipped" => {
	if (bottom <= 0) {
		return "above";
	}
	if (top >= screenHeight) {
		return "below";
	}
	if (top < 0 || bottom > screenHeight) {
		return "clipped";
	}
	return "visible";
};

const round = (value: number) => Math.round(value * 10) / 10;

// ---------------------------------------------------------------------------
// The running sample buffer. Fed by a passive touch listener (see
// stickyHeaderWatchdog.ts); read when a report is built.

const MAX_SAMPLES = 24;
const samples: TouchSample[] = [];

export const recordTouchSample = (sample: TouchSample) => {
	samples.push(sample);
	while (samples.length > MAX_SAMPLES) {
		samples.shift();
	}
};

export const getTouchSamples = (): TouchSample[] => [...samples];

export const resetTouchSamplesForTesting = () => {
	samples.length = 0;
};
