import { useEffect, useState } from "react";
import { isWideFloor } from "../courtTemplate.ts";

// IS A FLOOR PICTURE A WIDE ONE (see isWideFloor)? Its shape tells, once it
// has loaded - undefined until then. Each picture is looked at once, every
// court showing it after that knowing straight away (the 2D court and the 3D
// one's picture of it alike, so the two lay it down the same).

const known = new Map<string, boolean>();
const looking = new Map<string, Promise<boolean>>();

const look = (url: string): Promise<boolean> => {
	let p = looking.get(url);
	if (!p) {
		p = new Promise<boolean>((resolve) => {
			const img = new Image();
			img.onload = () => {
				resolve(isWideFloor(img.naturalWidth, img.naturalHeight));
			};
			img.onerror = () => {
				resolve(false);
			};
			img.src = url;
		}).then((wide) => {
			// (A handful of floors a session: kept to a handful.)
			if (known.size > 40) {
				known.clear();
			}
			known.set(url, wide);
			looking.delete(url);
			return wide;
		});
		looking.set(url, p);
	}
	return p;
};

export const useWideFloor = (url: string | undefined): boolean | undefined => {
	const [, seen] = useState(0);
	useEffect(() => {
		if (!url || known.has(url)) {
			return;
		}
		let alive = true;
		void look(url).then(() => {
			if (alive) {
				seen((n) => n + 1);
			}
		});
		return () => {
			alive = false;
		};
	}, [url]);
	return url ? known.get(url) : false;
};
