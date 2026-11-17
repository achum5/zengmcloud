// Run when this device takes over simming in a shared league. The smart CPU
// keeps a picture of the league it built while following (its outlook and the
// players it read into it); the device now deciding drops it and builds its
// own from the league as it now stands (docs/smart-cpu.md, shared leagues).
export const becameSimmerHooks: (() => void)[] = [];

export const runBecameSimmerHooks = () => {
	for (const hook of becameSimmerHooks) {
		hook();
	}
};
