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
	// through the route.
	const pageProps = (page: number, enabled: boolean) => {
		if (pageUrl) {
			return { href: enabled ? pageUrl(page) : undefined };
		}
		return {
			onClick: () => {
				if (enabled) {
					onClick(page);
				}
			},
		};
	};

	const numberedPages: ReactNode[] = [];

	for (let i = firstShownPage; i <= lastShownPage; i++) {
		numberedPages.push(
			<li
				key={i}
				className={clsx("page-item", i === currentPage ? "active" : null)}
			>
				{i === currentPage ? (
					<span
						className="page-link user-select-none"
						onClick={() => onClick(i)}
					>
						{i}
					</span>
				) : (
					<a className="page-link user-select-none" {...pageProps(i, true)}>
						{i}
					</a>
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
				<a
					className="page-link user-select-none"
					{...pageProps(currentPage - 1, showPrev)}
				>
					← Prev
				</a>
			</li>
			{numberedPages}
			<li
				className={clsx("page-item", {
					disabled: !showNext,
				})}
			>
				<a
					className="page-link user-select-none"
					{...pageProps(currentPage + 1, showNext)}
				>
					Next →
				</a>
			</li>
		</ul>
	);
};

export default Pagination;
