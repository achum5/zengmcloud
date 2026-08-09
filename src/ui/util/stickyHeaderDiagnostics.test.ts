import { assert, describe, test } from "vitest";
import {
	formatHeaderReport,
	getHeaderLog,
	recordHeaderEvent,
} from "./stickyHeaderDiagnostics.ts";

describe("formatHeaderReport", () => {
	test("puts the snapshot and the log in one pasteable block", () => {
		const report = formatHeaderReport(
			{ headerTop: -412, headerPosition: "sticky", modalPinned: false },
			[
				{ at: 1200, kind: "detached", scrollY: 412, headerTop: -412 },
				{
					at: 1250,
					kind: "repaired",
					scrollY: 412,
					headerTop: 0,
					detail: "step=2",
				},
			],
		);
		assert.ok(report.includes("headerTop: -412"), report);
		assert.ok(report.includes("modalPinned: false"), report);
		assert.ok(
			report.includes("1200 detached scrollY=412 headerTop=-412"),
			report,
		);
		assert.ok(
			report.includes("1250 repaired scrollY=412 headerTop=0 step=2"),
			report,
		);
	});

	test("says so plainly when nothing ever went wrong", () => {
		// An empty log is itself a finding - it means the fault never tripped the
		// detector, which points somewhere different than a failed repair.
		const report = formatHeaderReport({ headerTop: 0 }, []);
		assert.ok(report.includes("(empty"), report);
	});

	test("renders a missing snapshot value rather than 'undefined'", () => {
		const report = formatHeaderReport({ headerPosition: undefined }, []);
		assert.ok(report.includes("headerPosition: -"), report);
	});
});

describe("recordHeaderEvent", () => {
	test("keeps the log bounded so it stays pasteable", () => {
		for (let i = 0; i < 200; i++) {
			recordHeaderEvent({
				kind: "detached",
				scrollY: i,
				headerTop: -i,
			});
		}
		const log = getHeaderLog();
		assert.ok(log.length <= 60, `log grew to ${log.length}`);
		// Oldest dropped, newest kept.
		assert.strictEqual(log.at(-1)!.scrollY, 199);
	});

	test("stamps entries with a time", () => {
		recordHeaderEvent({ kind: "forced", scrollY: 10, headerTop: 0 });
		assert.strictEqual(typeof getHeaderLog().at(-1)!.at, "number");
	});
});
