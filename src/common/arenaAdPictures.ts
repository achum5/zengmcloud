import type { CourtStyle } from "./types.ts";

// A court's arena ads' pictures (see CourtStyle.arenaAds) - or a football
// stadium's (StadiumLook.arenaAds): the ones uploaded to the league, by id -
// and the ads with each of those filled in from `pictures` (one not there
// left without its picture).
const PIC = "pic:";

export const arenaAdPictureIds = (
	court: Pick<CourtStyle, "arenaAds"> | undefined,
): string[] =>
	(court?.arenaAds ?? [])
		.map((a) => a.imageURL)
		.filter((u): u is string => typeof u === "string" && u.startsWith(PIC))
		.map((u) => u.slice(PIC.length));

export const withArenaAdPictures = (
	ads: NonNullable<CourtStyle["arenaAds"]>,
	pictures: Record<string, string>,
): NonNullable<CourtStyle["arenaAds"]> =>
	ads.map((a) => {
		if (!a.imageURL?.startsWith(PIC)) {
			return a;
		}
		const url = pictures[a.imageURL.slice(PIC.length)];
		const { imageURL: _, ...rest } = a;
		return url === undefined ? rest : { ...rest, imageURL: url };
	});
