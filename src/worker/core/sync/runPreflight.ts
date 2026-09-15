import { deleteApp, initializeApp } from "firebase/app";
import { getAuth, signInAnonymously } from "firebase/auth";
import {
	deleteDoc,
	doc,
	getDocFromServer,
	getFirestore,
	setDoc,
} from "firebase/firestore";
import type { FirebaseConfig } from "./firebaseConfig.ts";
import type {
	PreflightResult,
	PreflightStep,
} from "../../../common/preflight.ts";
import {
	classifyPreflightError,
	errorText,
} from "../../../common/preflightProblem.ts";

// CHECKING SOMEONE ELSE'S FIREBASE PROJECT BEFORE THEY TRUST A LEAGUE TO IT.
//
// The steps run in the order they depend on each other, against a throwaway
// document, so the first thing that is wrong is the thing reported - rather
// than a connect failing later with whatever the SDK happened to say.
//
// The probe writes a control doc stamped with this device's uid, which is
// exactly the shape of write the league itself makes. A pass therefore means
// the rules allow what sync actually needs, not something adjacent to it.
//
// What each failure MEANS lives in common/preflightProblem.ts, kept pure so the
// decision table can be tested without a live project (which no test can have).

const PROBE_ROOM = "_zengm_preflight";

// Named for the run and deleted afterwards: a half-configured project must not
// leave a live Firebase app behind for the real connect to pick up.
const probeAppName = () => `preflight-${Date.now()}-${Math.random()}`;

export const preflightFirebaseConfig = async (
	config: FirebaseConfig,
): Promise<PreflightResult> => {
	const steps: PreflightResult["steps"] = [];
	const pass = (step: PreflightStep) => {
		steps.push({ step, ok: true });
	};
	const fail = (step: PreflightStep, error: unknown): PreflightResult => {
		steps.push({ step, ok: false });
		return {
			ok: false,
			steps,
			...classifyPreflightError(step, error),
			detail: errorText(error),
		};
	};

	const app = initializeApp(config, probeAppName());
	try {
		pass("project");

		// 1. Anonymous sign-in. Everything else needs an identity, and this is the
		//    checkbox people miss most often.
		let uid: string;
		try {
			uid = (await signInAnonymously(getAuth(app))).user.uid;
			pass("auth");
		} catch (error) {
			return fail("auth", error);
		}

		const probe = doc(getFirestore(app), "leagues", PROBE_ROOM, "control", uid);

		// 2. A write shaped exactly like the league's own.
		try {
			await setDoc(probe, { holderId: uid, at: Date.now() });
			pass("write");
		} catch (error) {
			return fail("write", error);
		}

		// 3. Read it back FROM THE SERVER, which is what catching up does. A plain
		//    read would be answered from the SDK's own cache and pass even where
		//    the rules forbid it.
		try {
			const snap = await getDocFromServer(probe);
			if (!snap.exists()) {
				throw new Error("the probe document did not come back");
			}
			pass("read");
		} catch (error) {
			return fail("read", error);
		}

		// 4. Tidy up. A leftover probe doc is harmless, so this never fails the
		//    run - it is reported only so rules that forbid deletes are visible
		//    now rather than when a league tries to clean up after itself.
		try {
			await deleteDoc(probe);
			pass("cleanup");
		} catch {
			steps.push({ step: "cleanup", ok: false });
		}

		return { ok: true, steps };
	} finally {
		try {
			await deleteApp(app);
		} catch {
			// The app is throwaway; failing to tear it down changes nothing.
		}
	}
};
