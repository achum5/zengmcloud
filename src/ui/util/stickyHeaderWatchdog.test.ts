import { assert, describe, test } from "vitest";
import {
	bottomBarIsDetached,
	detachmentConfirmed,
	headerIsDetached,
	repairCanHelp,
	repairVerdict,
	runExclusive,
	scrollDecision,
	SETTLE_MS,
	tickerAnchorHeights,
	UNPIN_CHECK_DELAYS_MS,
	viewportOffsetIsInformative,
} from "./stickyHeaderWatchdog.ts";

const deps = ({
	scrollY = 500,
	headerTop = 0,
	stickyTop = 0,
	pinnedByModal = false,
	visualOffsetTop = 0,
}: {
	scrollY?: number;
	headerTop?: number;
	stickyTop?: number;
	pinnedByModal?: boolean;
	visualOffsetTop?: number;
}) => ({
	scrollY: () => scrollY,
	headerTop: () => headerTop,
	stickyTop: () => stickyTop,
	pinnedByModal: () => pinnedByModal,
	visualOffsetTop: () => visualOffsetTop,
});

describe("headerIsDetached", () => {
	// The failure this exists for: the page is scrolled, and the header has gone
	// up the screen with it instead of staying at the top.
	test("a header that scrolled away with the page is detached", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: -500 })),
			true,
		);
	});

	test("a header holding the top is not", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: 0 })),
			false,
		);
	});

	// At the top of the document a stuck header and an unstuck one sit in exactly
	// the same place, so there is nothing to compare. Guessing here would mean
	// rebuilding the sticky node on every single resume, working or not.
	test("nothing is claimed while the page is at the top", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 0, headerTop: 0 })),
			false,
		);
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 0, headerTop: -80 })),
			false,
		);
	});

	// Sub-pixel rounding and zoom mean the rect is never exactly on the line, and
	// a fraction of a pixel is not a broken header.
	test("sub-pixel drift is not a fault", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: -1 })),
			false,
		);
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: -1.5 })),
			false,
		);
	});

	test("a genuine break is well past the tolerance", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: -3 })),
			true,
		);
	});

	// A header that sticks below something else (any future offset) is judged
	// against its own top, not against zero.
	test("it respects the header's own sticky offset", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: 40, stickyTop: 40 })),
			false,
		);
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: 0, stickyTop: 40 })),
			true,
		);
	});

	// While a modal pins the body the header is deliberately position:fixed. That
	// is not a fault, and rebuilding a sticky node underneath it would fight the
	// modal.
	test("a modal's deliberate pin is left alone", () => {
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 500, headerTop: -500, pinnedByModal: true }),
			),
			false,
		);
	});

	// Some browsers report a NaN rect for an element that isn't laid out yet.
	// Acting on that would rebuild the node for no reason.
	test("unmeasurable geometry claims nothing", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: Number.NaN, headerTop: -500 })),
			false,
		);
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: Number.NaN })),
			false,
		);
	});

	// A header BELOW where it sticks is a page mid-scroll-into-place, not a
	// broken one - never repair that.
	test("a header below its sticky point is not detached", () => {
		assert.strictEqual(
			headerIsDetached(deps({ scrollY: 500, headerTop: 30 })),
			false,
		);
	});
});

// The header is undetectable at the top of the page, so scrolling is the only
// event that can ever catch it - but NOT while the scroll is still running. On
// iOS the main thread's geometry lags the compositor during a flick, so a
// healthy sticky header reads exactly like a broken one. A field log showed
// every single detection taken mid-scroll, so the watch now waits for the page
// to settle and every fault is confirmed against a second reading.
// getBoundingClientRect() measures against the VISUAL viewport while sticky
// anchors to the LAYOUT one, so zooming or panning the two apart moves a
// perfectly healthy header to -offsetTop. A field log caught exactly this and
// the watchdog spent it running an impossible repair four times a second.
describe("headerIsDetached with the viewports panned apart", () => {
	test("a header at exactly -offsetTop is doing its job, not broken", () => {
		// The field numbers: 646-tall visual viewport 406px down a 1052 layout
		// viewport, header measured at -406.
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 864, headerTop: -406, visualOffsetTop: 406 }),
			),
			false,
		);
	});

	test("a genuinely detached header is still caught while zoomed", () => {
		// Gone up with the page rather than parked at the offset.
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 864, headerTop: -864, visualOffsetTop: 406 }),
			),
			true,
		);
	});

	test("without an offset the old comparison is unchanged", () => {
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 500, headerTop: -500, visualOffsetTop: 0 }),
			),
			true,
		);
	});
});

// THE BLIND SPOT THAT MADE THE REPAIR BUTTON USELESS.
//
// Subtracting visualViewport.offsetTop excuses a panned page. But when that
// offset is the SAME NUMBER as the scroll position, the excuse covers a header
// lying at its own static position - which is the definition of a header that
// has come unstuck - and the watchdog reports "healthy" however far the bar has
// ridden. Two field reports were in exactly this state, both with the owner
// saying the header scrolled off the screen and the manual repair did nothing.
describe("headerIsDetached when the offset merely echoes the scroll", () => {
	// The field report, to the pixel: scrolled to 14, header at -14, offsetTop
	// 14. Before this rule the answer was false and the ladder never ran.
	test("a header at its static position is detached, echo or not", () => {
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 14, headerTop: -14, visualOffsetTop: 14 }),
			),
			true,
		);
	});

	test("and at the larger scroll the same shape showed up at", () => {
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 406, headerTop: -406, visualOffsetTop: 406 }),
			),
			true,
		);
	});

	// The excuse still has to work where it was earned, which is an offset that
	// says something the scroll position does not.
	test("a genuine pan is still excused", () => {
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 864, headerTop: -406, visualOffsetTop: 406 }),
			),
			false,
		);
	});

	// Rounding, not a different number.
	test("an offset a pixel off the scroll is still an echo", () => {
		assert.strictEqual(
			headerIsDetached(
				deps({ scrollY: 300, headerTop: -300, visualOffsetTop: 301 }),
			),
			true,
		);
	});
});

// The ticker has the same blind spot, and it matters more there: the offset
// candidates are added to a set that only has to be matched ONCE for the bar to
// pass, so an echoing offset vouches for a bar at any position at all.
describe("bottomBarIsDetached when the offset merely echoes the scroll", () => {
	test("a bar riding the content is caught despite the echo", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({
					barBottom: 1038,
					layoutHeight: 1052,
					visualOffsetTop: 14,
					scrollY: 14,
				}),
			),
			true,
		);
	});

	test("a genuine pan still gets the bar off the hook", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({
					barBottom: 646,
					layoutHeight: 1052,
					visualOffsetTop: 406,
					scrollY: 0,
				}),
			),
			false,
		);
	});
});

// THE LADDER ONLY WORKS ON A STALE ELEMENT, and the probe is how that gets
// checked before three rebuilds and a scroll nudge go to waste on a bar with
// nothing wrong with it.
describe("repairCanHelp", () => {
	test("a probe where it belongs means the bar alone is stale - rebuild it", () => {
		assert.strictEqual(repairCanHelp({ probeTop: 0, scrollY: 400 }), true);
	});

	test("a probe adrift means sticky is broken page-wide - do not bother", () => {
		assert.strictEqual(repairCanHelp({ probeTop: -400, scrollY: 400 }), false);
	});

	// At the top of the page a working sticky element and a broken one are in the
	// same place, so the probe cannot convict anything.
	test("at the top of the page the probe says nothing", () => {
		assert.strictEqual(repairCanHelp({ probeTop: 0, scrollY: 0 }), true);
	});

	test("no probe is no evidence, so the ladder still runs", () => {
		assert.strictEqual(
			repairCanHelp({ probeTop: undefined, scrollY: 400 }),
			true,
		);
		assert.strictEqual(
			repairCanHelp({ probeTop: Number.NaN, scrollY: 400 }),
			true,
		);
	});

	test("a pixel of rounding is not a probe adrift", () => {
		assert.strictEqual(repairCanHelp({ probeTop: -1, scrollY: 400 }), true);
	});
});

describe("viewportOffsetIsInformative", () => {
	test("zero says nothing", () => {
		assert.strictEqual(
			viewportOffsetIsInformative({ offset: 0, scrollY: 0 }),
			false,
		);
	});

	test("an offset that is the scroll position says nothing", () => {
		assert.strictEqual(
			viewportOffsetIsInformative({ offset: 240, scrollY: 240 }),
			false,
		);
	});

	test("an offset the scroll position cannot explain says something", () => {
		assert.strictEqual(
			viewportOffsetIsInformative({ offset: 406, scrollY: 864 }),
			true,
		);
	});

	// An unreadable scroll position cannot convict the offset of echoing it.
	test("an unmeasurable scroll leaves the offset trusted", () => {
		assert.strictEqual(
			viewportOffsetIsInformative({ offset: 406, scrollY: Number.NaN }),
			true,
		);
	});

	test("an unmeasurable offset is never used", () => {
		assert.strictEqual(
			viewportOffsetIsInformative({ offset: Number.NaN, scrollY: 10 }),
			false,
		);
	});
});

describe("scrollDecision", () => {
	test("a scroll far enough down the page is worth measuring", () => {
		assert.strictEqual(scrollDecision({ scrollY: 500 }), "judge");
	});

	test("says nothing at the top, where the question is unanswerable", () => {
		assert.strictEqual(scrollDecision({ scrollY: 0 }), "at-top");
		assert.strictEqual(
			scrollDecision({ scrollY: 1 }),
			"at-top",
			"within tolerance is still the top",
		);
	});

	test("a nonsense scroll position claims nothing", () => {
		assert.strictEqual(scrollDecision({ scrollY: Number.NaN }), "at-top");
	});
});

describe("detachmentConfirmed", () => {
	test("two readings at the same offset that agree is a real fault", () => {
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 400, edge: -400 },
				after: { scrollY: 400, edge: -400 },
			}),
			true,
		);
	});

	test("a page that moved between readings proves nothing", () => {
		// The exact shape of the phantom faults in the field log: detected at
		// scrollY 69, re-measured at 149. Mid-flick, so unanswerable.
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 69, edge: -69 },
				after: { scrollY: 149, edge: -149 },
			}),
			false,
		);
	});

	test("a header still moving at a settled offset is not confirmed", () => {
		// Same scroll offset but the rect is still catching up - the compositor
		// had not finished, so wait rather than tear the header apart.
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 400, edge: -400 },
				after: { scrollY: 400, edge: -120 },
			}),
			false,
		);
	});

	test("sub-pixel disagreement is still a confirmation", () => {
		assert.strictEqual(
			detachmentConfirmed({
				before: { scrollY: 400, edge: -400 },
				after: { scrollY: 400, edge: -401 },
			}),
			true,
		);
	});
});

// THE BOTTOM TICKER. Same disease as the header, different edge: what has to
// hold still is its bottom against the foot of the viewport.
const bottomDeps = ({
	barBottom = 800,
	layoutHeight = 800,
	anchorHeights = undefined as number[] | undefined,
	pinnedByModal = false,
	visualOffsetTop = 0,
	scrollY = 0,
}: {
	barBottom?: number;
	layoutHeight?: number;
	anchorHeights?: number[];
	pinnedByModal?: boolean;
	visualOffsetTop?: number;
	scrollY?: number;
}) => ({
	barBottom: () => barBottom,
	anchorHeights: () => anchorHeights ?? [layoutHeight],
	pinnedByModal: () => pinnedByModal,
	visualOffsetTop: () => visualOffsetTop,
	scrollY: () => scrollY,
});

describe("bottomBarIsDetached", () => {
	test("a bar sitting on the foot of the viewport is fine", () => {
		assert.strictEqual(bottomBarIsDetached(bottomDeps({})), false);
	});

	// The reported failure: the ticker scrolled up the page with the document and
	// stayed floating in the middle of the screen.
	test("a bar that floated up the page is detached", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: 430, layoutHeight: 800 })),
			true,
		);
	});

	// A stale compositor rule pins the bar where it last was, and the page can
	// then scroll either way underneath it.
	test("a bar left below the viewport is detached too", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: 1100, layoutHeight: 800 })),
			true,
		);
	});

	// iOS moves the viewport height by a few pixels while the URL bar collapses.
	// That is the browser working, not the bar breaking.
	test("a few pixels of viewport churn is not a fault", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: 795, layoutHeight: 800 })),
			false,
		);
	});

	// Unlike the header, a bottom bar is just as measurable at the top of the
	// document as anywhere else - there is no ambiguous position.
	test("it answers at the top of the document, where the header cannot", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: 200, layoutHeight: 800 })),
			true,
		);
	});

	// getBoundingClientRect reports against the visual viewport while fixed
	// positions against the layout one, so a healthy bar on a panned page reads
	// short by exactly the offset between them.
	test("a bar short by exactly the visual offset is doing its job", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({
					barBottom: 646,
					layoutHeight: 1052,
					visualOffsetTop: 406,
				}),
			),
			false,
		);
	});

	test("a genuinely detached bar is still caught while panned", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({
					barBottom: 300,
					layoutHeight: 1052,
					visualOffsetTop: 406,
				}),
			),
			true,
		);
	});

	// The repair ladder nudges the scroll position, which is the last thing a
	// modal-pinned page needs. The unpin check picks it up afterwards.
	test("a modal's pin is left alone", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({ barBottom: 200, layoutHeight: 800, pinnedByModal: true }),
			),
			false,
		);
	});

	test("unmeasurable geometry claims nothing", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ barBottom: Number.NaN })),
			false,
		);
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ layoutHeight: 0 })),
			false,
		);
	});

	// THE FALSE-DETACHMENT INCIDENT. On iOS, documentElement.clientHeight stays
	// at the small viewport (URL bar showing) while the layout viewport sticky is
	// anchored to grows by the toolbar's height the moment the user scrolls. The
	// first version measured against clientHeight alone, so scrolling put every
	// HEALTHY ticker 60-100px "off", and the repair ladder went to work on a bar
	// with nothing wrong - parking it mid-content for a frame, blinking it, and
	// nudging the scroll, over and over. The bar must be judged against every
	// height the foot of the viewport can legitimately be at.
	test("a healthy bar under a collapsed iOS toolbar is not a fault", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({ barBottom: 812, anchorHeights: [750, 812, 812] }),
			),
			false,
		);
	});

	test("a bar pinned to the visible bottom by the standing correction is fine", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({ barBottom: 646, anchorHeights: [1052, 1052, 646] }),
			),
			false,
		);
	});

	test("a genuinely detached bar is far from every legitimate anchor", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({ barBottom: 430, anchorHeights: [750, 812, 812] }),
			),
			true,
		);
	});

	test("an anchor list with no usable entries claims nothing", () => {
		assert.strictEqual(
			bottomBarIsDetached(
				bottomDeps({ barBottom: 430, anchorHeights: [Number.NaN, 0] }),
			),
			false,
		);
	});
});

// THE TWO MECHANISMS HAVE TO AGREE. The standing correction lifts the bar to
// the foot of the visible area; this asks whether the bar is somewhere a
// healthy bar can be. If the two disagree the watchdog tears down a bar that is
// exactly where it was just put - the false-detachment incident with a new
// cause - so the field numbers are pinned here.
describe("the standing correction and the watchdog agree", () => {
	// offsetTop 57, layout 1052, visual 646. Sticky leaves the bar at 995 and
	// the correction lifts it to 646, which is the vvBottom anchor exactly.
	const FIELD = {
		anchorHeights: [1052, 1052, 703],
		visualOffsetTop: 57,
	};

	test("a corrected bar is not called detached", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ ...FIELD, barBottom: 646 })),
			false,
		);
	});

	// The frame before the correction lands, sticky has it at the foot of the
	// layout viewport - also a place a healthy bar can be, so no flapping.
	test("an uncorrected bar is not called detached either", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ ...FIELD, barBottom: 995 })),
			false,
		);
	});

	test("a bar riding the content is still caught", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ ...FIELD, barBottom: 400 })),
			true,
		);
	});
});

// THE PHANTOM OFFSET, from a field log: offsetTop 240 on a 1083-tall layout
// viewport, with the ticker sitting at 1083 - exactly where sticky puts it, no
// transform on it - and the header, measured in the same breath and also
// untransformed, reading 0 rather than -240. The rects knew nothing about that
// offset. Subtracting it put the expected foot at 843, so the watchdog called a
// perfectly placed bar detached and ran the repair ladder on it.
describe("a phantom visual-viewport offset does not fake a detachment", () => {
	const FIELD = { anchorHeights: [1083, 1083, 876], visualOffsetTop: 240 };

	test("a bar at the un-offset foot is healthy", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ ...FIELD, barBottom: 1083 })),
			false,
		);
	});

	// And the other world still works: on a genuinely panned page the bar reads
	// short by the offset, and that is healthy too.
	test("a bar at the offset foot is also healthy", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ ...FIELD, barBottom: 843 })),
			false,
		);
	});

	test("the visible-bottom anchor still counts", () => {
		assert.strictEqual(
			bottomBarIsDetached(bottomDeps({ ...FIELD, barBottom: 636 })),
			false,
		);
	});

	// The check still has to earn its keep: a bar riding the content is nowhere
	// near any candidate, with or without the offset.
	test("a bar adrift in the content is still caught", () => {
		for (const barBottom of [400, 550, 1400]) {
			assert.strictEqual(
				bottomBarIsDetached(bottomDeps({ ...FIELD, barBottom })),
				true,
				String(barBottom),
			);
		}
	});
});

describe("tickerAnchorHeights", () => {
	test("normal life keeps all three anchors", () => {
		// Toolbar collapsed: innerHeight grows past clientHeight by well under the
		// stale bar - both stay, which is the false-detachment lesson.
		assert.deepStrictEqual(
			tickerAnchorHeights({ client: 660, innerHeight: 750, vvBottom: 750 }),
			[660, 750, 750],
		);
	});

	test("a snapshot-sized clientHeight is thrown out", () => {
		// The resume fault: clientHeight restored at the app-switcher snapshot's
		// size. Left in, a bar parked at 580 measures healthy and nothing repairs.
		const anchors = tickerAnchorHeights({
			client: 580,
			innerHeight: 830,
			vvBottom: 830,
		});
		assert.ok(Number.isNaN(anchors[0]!));
		assert.deepStrictEqual(anchors.slice(1), [830, 830]);
	});

	test("a keyboard shrinking innerHeight does not disqualify clientHeight", () => {
		// Only the small side is distrusted - innerHeight below clientHeight is
		// what a keyboard does, and the bar parked behind it is the chosen answer.
		assert.deepStrictEqual(
			tickerAnchorHeights({ client: 830, innerHeight: 530, vvBottom: 530 }),
			[830, 530, 530],
		);
	});
});

describe("runExclusive", () => {
	const deferred = () => {
		let resolve!: () => void;
		const promise = new Promise<void>((r) => {
			resolve = r;
		});
		return { promise, resolve };
	};

	// The bug this exists for: the claim used to be taken on the far side of the
	// frame the check waits to confirm a detachment, so two checks starting in
	// the same frame both got through and both ran a repair ladder on the same
	// element.
	test("a second caller is turned away while the first is still awaiting", async () => {
		const gate = deferred();
		let runs = 0;
		const first = runExclusive("header", async () => {
			runs += 1;
			await gate.promise;
		});
		const second = await runExclusive("header", async () => {
			runs += 1;
		});
		assert.strictEqual(second, false);
		assert.strictEqual(runs, 1);
		gate.resolve();
		assert.strictEqual(await first, true);
	});

	test("the bars do not block each other", async () => {
		const gate = deferred();
		const header = runExclusive("header", () => gate.promise);
		assert.strictEqual(await runExclusive("ticker", async () => {}), true);
		gate.resolve();
		await header;
	});

	test("the claim is released once the body settles, even on a throw", async () => {
		let threw = false;
		try {
			await runExclusive("header", async () => {
				throw new Error("ladder blew up");
			});
		} catch {
			threw = true;
		}
		assert.ok(threw);
		assert.strictEqual(await runExclusive("header", async () => {}), true);
	});
});

describe("repairVerdict", () => {
	test("a step that fixed it, and stayed fixed, is a repair", () => {
		assert.strictEqual(
			repairVerdict({
				detachedBefore: false,
				readingsAgree: true,
				detachedAfter: false,
			}),
			"held",
		);
	});

	test("a step that changed nothing keeps the ladder going", () => {
		assert.strictEqual(
			repairVerdict({
				detachedBefore: true,
				readingsAgree: true,
				detachedAfter: true,
			}),
			"still-broken",
		);
	});

	test("clean now, broken a frame later, is not a repair", () => {
		assert.strictEqual(
			repairVerdict({
				detachedBefore: false,
				readingsAgree: true,
				detachedAfter: true,
			}),
			"still-broken",
		);
	});

	test("clean twice while the page moved proves nothing", () => {
		// The case from the field log: "repaired step=2" at a headerTop identical
		// to the one that declared the fault. Either the ladder worked or the
		// viewport panned underneath it, and a single reading cannot say which.
		assert.strictEqual(
			repairVerdict({
				detachedBefore: false,
				readingsAgree: false,
				detachedAfter: false,
			}),
			"unclear",
		);
	});

	test("a bar still adrift is still adrift however the page moved", () => {
		assert.strictEqual(
			repairVerdict({
				detachedBefore: true,
				readingsAgree: false,
				detachedAfter: true,
			}),
			"still-broken",
		);
	});
});

describe("the modal unpin waits for its own scroll to settle", () => {
	test("every check lands outside the window a reading is worthless in", () => {
		// Unpinning ends with a scrollTo of hundreds of pixels. Two of the three
		// checks used to run at the next frame and at 50ms, inside the window
		// SETTLE_MS exists to exclude, where a healthy header reads exactly like
		// a broken one.
		assert.isAbove(UNPIN_CHECK_DELAYS_MS.length, 0);
		for (const delay of UNPIN_CHECK_DELAYS_MS) {
			assert.isAbove(delay, SETTLE_MS, `unpin check at ${delay}ms`);
		}
	});
});
