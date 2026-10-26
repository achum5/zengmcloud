import clsx from "clsx";
import type { ReactNode } from "react";

const Pagination = ({
	currentPage,
	numRows,
	onClick,
	perPage,
	pageUrl,
}: {
	currentPage: number;
	numRows: number;
	onClick: (a: number) => void;
	perPage: number;
	// When a table's page number lives in the URL, this builds the address for a
	// given page. Supplying it turns the pager into real links - so a page can be
	// bookmarked, shared, opened in a new tab, and walked with the back button -
	// instead of buttons that only mutate in-memory state. Without it the pager
	// behaves as it always has.
	pageUrl?: (page: number) => string;
}) => {
	const showPrev = currentPage > 1;
	const showNext = numRows > currentPage * perPage;
	const numPages = Math.ceil(numRows / perPage);
	let firstShownPage = currentPage <= 3 ? 1 : currentPage - 2;

	while (firstShownPage > 1 && numPages - firstShownPage < 4) {
		firstShownPage -= 1;
	}

	let lastShownPage = firstShownPage + 4;

	if (lastShownPage > numPages) {
		lastShownPage = numPages;
	}

	// With pageUrl, the pager is plain links and the ROUTER does the work - the
	// URL is the source of truth for which page is showing, so it can be
	// bookmarked, shared, middle-clicked and walked with the back button. No
	// onClick: handling it here as well would page twice, once in state and once
	// through the route. Without it, buttons.
	const pageLink = (page: number, enabled: boolean, children: ReactNode) => {
		if (pageUrl) {
			return (
				<a
					className="page-link user-select-none"
					href={enabled ? pageUrl(page) : undefined}
					aria-disabled={enabled ? undefined : true}
				>
					{children}
				</a>
			);
		}
		return (
			<button
				type="button"
				className="page-link user-select-none"
				disabled={!enabled}
				onClick={() => onClick(page)}
			>
				{children}
			</button>
		);
	};

	const numberedPages: ReactNode[] = [];

	for (let i = firstShownPage; i <= lastShownPage; i++) {
		numberedPages.push(
			<li
				key={i}
				className={clsx("page-item", i === currentPage ? "active" : null)}
			>
				{i === currentPage ? (
					<span className="page-link user-select-none" aria-current="page">
						{i}
					</span>
				) : (
					pageLink(i, true, i)
				)}
			</li>,
		);
	}

	return (
		<ul className="pagination mb-0 ms-auto">
			<li
				className={clsx("page-item", {
					disabled: !showPrev,
				})}
			>
				{pageLink(currentPage - 1, showPrev, "← Prev")}
			</li>
			{numberedPages}
			<li
				className={clsx("page-item", {
					disabled: !showNext,
				})}
			>
				{pageLink(currentPage + 1, showNext, "Next →")}
			</li>
		</ul>
	);
};

export default Pagination;
