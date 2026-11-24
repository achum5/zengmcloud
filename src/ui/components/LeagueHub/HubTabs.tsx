import clsx from "clsx";
import { useEffect, useRef, useState, type RefObject } from "react";

// THE HUB ON A PHONE, IN TABS: the night's scores, its recap and the
// standings, each about a screen, picked in a row pinned under the navbar.
// (On a wider screen the hub is one page and there are no tabs.)

export type HubTabId = "recap" | "scores" | "standings";

const LABELS: Record<HubTabId, string> = {
	recap: "Recap",
	scores: "Scores",
	standings: "Standings",
};

// (The tab picked, kept from night to night while the game is open.)
let picked: HubTabId | undefined;

// The tab shown: the one picked if this night has it, else its first.
export const useHubTab = (tabs: HubTabId[]) => {
	const [choice, setChoice] = useState(picked);
	const tab =
		choice !== undefined && tabs.includes(choice)
			? choice
			: (tabs[0] ?? "scores");
	const choose = (id: HubTabId) => {
		picked = id;
		setChoice(id);
	};
	return [tab, choose] as const;
};

export const HubTabs = ({
	contentRef,
	onPick,
	tab,
	tabs,
}: {
	contentRef: RefObject<HTMLElement | null>;
	onPick: (id: HubTabId) => void;
	tab: HubTabId;
	tabs: HubTabId[];
}) => {
	const ref = useRef<HTMLDivElement>(null);
	// Pinned just under the navbar, however tall it is.
	const [top, setTop] = useState(0);
	useEffect(() => {
		const measure = () => {
			const nav = document.querySelector<HTMLElement>(".navbar.sticky-top");
			setTop(nav?.offsetHeight ?? 0);
		};
		measure();
		window.addEventListener("resize", measure);
		return () => {
			window.removeEventListener("resize", measure);
		};
	}, []);

	if (tabs.length < 2) {
		return null;
	}
	return (
		<div ref={ref} className="lh-tabbar" role="tablist" style={{ top }}>
			{tabs.map((id) => (
				<button
					key={id}
					type="button"
					role="tab"
					aria-selected={id === tab}
					className={clsx("lh-tabbar-tab", { active: id === tab })}
					onClick={() => {
						onPick(id);
						// Picked from further down: back up to the top of it.
						const content = contentRef.current;
						const bar = ref.current;
						if (content && bar) {
							const y =
								content.getBoundingClientRect().top +
								window.scrollY -
								top -
								bar.offsetHeight -
								12;
							if (window.scrollY > y) {
								window.scrollTo({ top: y });
							}
						}
					}}
				>
					{LABELS[id]}
				</button>
			))}
		</div>
	);
};
