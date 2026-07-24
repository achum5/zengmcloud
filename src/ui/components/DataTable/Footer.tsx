import clsx, { type ClassValue } from "clsx";
import { use, type ReactNode } from "react";
import { DataTableContext } from "./contexts.ts";

type FooterElement =
	| ReactNode
	| {
			value: ReactNode;
			classNames?: ClassValue;
	  };

export type FooterRow = {
	classNames?: ClassValue;
	data: FooterElement[];
};

const Footer = ({
	colOrder,
	footer,
	// Render as a bottom <tfoot> (default) or, for leading summary rows, a top
	// <tbody>. Either way the cells are column-order aware, so a summary row
	// lines up with the visible/reordered columns exactly like the data rows.
	tag: Tag = "tfoot",
}: {
	colOrder: {
		colIndex: number;
		hidden?: boolean;
	}[];
	footer?: FooterRow | FooterRow[];
	tag?: "tfoot" | "tbody";
}) => {
	if (!footer) {
		return null;
	}

	const { highlightCols } = use(DataTableContext);

	let footers: FooterRow[];

	if (Array.isArray(footer)) {
		// There are multiple footers
		footers = footer;
	} else {
		// There's only one footer
		footers = [footer];
	}

	return (
		<Tag>
			{footers.map((footer, i) => (
				<tr key={i} className={clsx(footer.classNames)}>
					{colOrder.map(({ colIndex }, j) => {
						const highlightColClassNames = highlightCols.includes(j)
							? "sorting_highlight"
							: undefined;

						const value = footer.data[colIndex];
						if (value != null && Object.hasOwn(value as any, "value")) {
							return (
								<th
									className={clsx(
										(value as any).classNames,
										highlightColClassNames,
									)}
									key={colIndex}
								>
									{(value as any).value}
								</th>
							);
						}

						return (
							<th key={colIndex} className={clsx(highlightColClassNames)}>
								{value as any}
							</th>
						);
					})}
				</tr>
			))}
		</Tag>
	);
};

export default Footer;
