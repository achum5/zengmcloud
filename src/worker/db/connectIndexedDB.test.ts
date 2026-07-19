import { assert, describe, test } from "vitest";
import { isTransientTransactionError } from "./connectIndexedDB.ts";

describe("isTransientTransactionError", () => {
	test("matches the iOS inactive-transaction hiccup that self-heals", () => {
		// The exact WebKit message + DOMException name for the reported bug.
		assert.strictEqual(
			isTransientTransactionError({
				name: "TransactionInactiveError",
				message: "Attempt to get a record from database without an in-progress transaction",
			}),
			true,
		);
		assert.strictEqual(
			isTransientTransactionError({
				name: "UnknownError",
				message: "Attempt to get a record from database without an in-progress transaction",
			}),
			true,
		);
		assert.strictEqual(
			isTransientTransactionError({ name: "InvalidStateError", message: "" }),
			true,
		);
		assert.strictEqual(
			isTransientTransactionError({
				name: "Error",
				message: "The transaction is inactive or finished.",
			}),
			true,
		);
	});

	test("does NOT match a real, actionable quota error", () => {
		// Genuine out-of-space must still surface its persistent toast.
		assert.strictEqual(
			isTransientTransactionError({
				name: "QuotaExceededError",
				message: "The quota has been exceeded.",
			}),
			false,
		);
	});

	test("does NOT match other genuine errors, or empty input", () => {
		assert.strictEqual(
			isTransientTransactionError({
				name: "ConstraintError",
				message: "Key already exists in the object store.",
			}),
			false,
		);
		assert.strictEqual(isTransientTransactionError(undefined), false);
		assert.strictEqual(isTransientTransactionError(null), false);
	});
});
