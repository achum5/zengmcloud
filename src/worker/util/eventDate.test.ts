import { beforeEach, describe, expect, test } from "vitest";
import { resetCache } from "../../test/helpers.ts";
import { duringNight, eventDate } from "./eventDate.ts";

// Today is the next night on the schedule; the sim's night overrides it while
// that night's moves are made.
const schedule = (dates: (string | undefined)[]) =>
	dates.map((date, i) => ({
		gid: i,
		day: i + 1,
		homeTid: 0,
		awayTid: 1,
		...(date === undefined ? {} : { date }),
	}));

describe("eventDate", () => {
	beforeEach(async () => {
		await resetCache({ schedule: schedule(["2027-10-20", "2027-10-21"]) });
	});

	test("is today on the calendar, outside the sim's night", async () => {
		expect(await eventDate()).toBe("2027-10-20");
	});

	test("is the night being simmed, while its moves are made", async () => {
		await duringNight("2027-10-19", async () => {
			expect(await eventDate()).toBe("2027-10-19");
		});
		expect(await eventDate()).toBe("2027-10-20");
	});

	test("goes back to today even when the night throws", async () => {
		await expect(
			duringNight("2027-10-19", async () => {
				throw new Error("boom");
			}),
		).rejects.toThrow("boom");
		expect(await eventDate()).toBe("2027-10-20");
	});

	test("is nothing on classic numbered days or between seasons", async () => {
		await resetCache({ schedule: schedule([undefined, undefined]) });
		expect(await eventDate()).toBeUndefined();
		await resetCache({ schedule: [] });
		expect(await eventDate()).toBeUndefined();
		await duringNight(undefined, async () => {
			expect(await eventDate()).toBeUndefined();
		});
	});
});
