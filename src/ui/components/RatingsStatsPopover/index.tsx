import clsx from "clsx";
import { type Ref, useCallback, useEffect, useState } from "react";
import { RatingsStats } from "./RatingsStats.tsx";
import { WatchBlock } from "../WatchBlock.tsx";
import { helpers } from "../../util/helpers.ts";
import { toWorker } from "../../util/toWorker.ts";
import { ResponsivePopover } from "../ResponsivePopover.tsx";
import { PLAYER } from "../../../common/constants.ts";
import { crossTabEmitter } from "../../util/crossTabEmitter.ts";
const Icon = ({
	onClick,
	ref,
	watch,
}: {
	onClick?: () => void;
	ref?: Ref<HTMLSpanElement>;
	watch: number;
}) => {
	return (
		<span
			ref={ref}
			className={clsx(
				"glyphicon glyphicon-stats watch",
				watch === 0 ? undefined : `watch-active-${watch}`,
			)}
			data-no-row-highlight="true"
			title="View ratings and stats"
			onClick={onClick}
		/>
	);
};

type Props = {
	// "default" means this is the default of an uncontrolled value, similar to defaultValue in React
	// undefined means "we don't know the watch value, so get it on initial load"
	defaultWatch?: number;
	disableNameLink?: boolean;
	pid: number;
	season?: number;
};

export const RatingsStatsPopover = ({
	defaultWatch,
	disableNameLink,
	pid,
	season,
}: Props) => {
	const [loadingData, setLoadingData] = useState<boolean>(false);
	const [failed, setFailed] = useState<boolean>(false);
	const [player, setPlayer] = useState<{
		abbrev?: string;
		tid?: number;
		age?: number;
		jerseyNumber?: string;
		name?: string;
		ratings?: {
			pos: string;
			ovr: number;
			pot: number;
			hgt: number;
			stre: number;
			spd: number;
			endu: number;
			season: number;
		};
		stats?: {
			[key: string]: number;
		};
		pid: number;
		type?: "career" | "current" | "draft" | number;
		// Whether the worker coarsened the ratings row above, so the gradient is
		// drawn on the same scale the numbers are actually on.
		coarseRatings?: boolean;
	}>({
		pid,
	});

	const [watch, setWatch] = useState(defaultWatch ?? 0);
	useEffect(() => {
		if (defaultWatch === undefined) {
			// Need to fetch initial value
			(async () => {
				const newLocalWatch = await toWorker("main", "getPlayerWatch", pid);
				setWatch(newLocalWatch);
			})();
		} else {
			// This happens when switching to a new pid but defaultWatch is supplied
			setWatch(defaultWatch);
		}

		// Need to listen for bulk action updates
		const unbind = crossTabEmitter.on("updateWatch", async (watchByPid) => {
			const newWatch = watchByPid[pid];
			if (newWatch !== undefined) {
				setWatch(newWatch);
			}
		});
		return unbind;
	}, [defaultWatch, pid]);

	// Object.is to handle NaN
	if (!Object.is(player.pid, pid)) {
		setLoadingData(false);
		setPlayer({
			pid,
		});
	}

	// Anything that can go wrong here has to leave the popover retryable. It
	// used to fire this without a catch, so a rejected call - or a worker guard
	// answering `undefined`, which the destructuring below then threw on - left
	// loadingData stuck true forever. toggle() is gated on !loadingData, so the
	// popover could never try again: it opened blank for the rest of the
	// session, on every player.
	const loadData = useCallback(async () => {
		try {
			const p = await toWorker("main", "ratingsStatsPopoverInfo", {
				pid,
				season,
			});
			if (!p) {
				throw new Error("No player info came back");
			}
			setPlayer({
				abbrev: p.abbrev,
				tid: p.tid,
				age: p.age,
				jerseyNumber: p.jerseyNumber,
				name: p.name,
				ratings: p.ratings,
				stats: p.stats,
				pid,
				type: p.type,
				coarseRatings: p.coarseRatings,
			});
			setFailed(false);
		} catch (error) {
			console.error("ratingsStatsPopoverInfo failed", error);
			setFailed(true);
		} finally {
			setLoadingData(false);
		}
	}, [pid, season]);

	const toggle = useCallback(() => {
		if (!loadingData) {
			loadData();
			setLoadingData(true);
		}
	}, [loadData, loadingData]);

	const {
		abbrev,
		tid,
		age,
		jerseyNumber,
		name,
		ratings,
		stats,
		type,
		coarseRatings,
	} = player;

	// JTODO: this probably makes a bit more sense as a component instead of a pure jsx function?
	let nameBlock = null;
	if (name) {
		nameBlock = (
			<div className="d-flex">
				{jerseyNumber ? (
					<div className="text-body-secondary jersey-number-popover align-self-end me-1">
						{jerseyNumber}
					</div>
				) : null}
				{disableNameLink ? (
					<b>{name}</b>
				) : (
					<a
						href={helpers.leagueUrl(["player", pid])}
						className="fw-bold text-truncate"
					>
						{name}
					</a>
				)}
				{ratings !== undefined ? (
					<div className="ms-1">{ratings.pos}</div>
				) : null}
				{abbrev !== undefined && tid !== undefined && tid !== PLAYER.RETIRED ? (
					<a
						href={helpers.leagueUrl([
							"roster",
							`${abbrev}_${tid}`,
							ratings ? ratings.season : undefined,
						])}
						className="ms-1"
					>
						{abbrev}
					</a>
				) : null}
				{age !== undefined ? (
					<div className="ms-1 flex-shrink-0">{age} yo</div>
				) : null}
				<WatchBlock
					pid={pid}
					watch={watch}
					onChange={(newWatch) => {
						// Update locally even though we'll get a crossTabEmitter event too, both for responsiveness and so it works in exhibition games
						setWatch(newWatch);
					}}
				/>
			</div>
		);
	}

	const id = `ratings-stats-popover-${player.pid}`;

	// An empty Ratings/Stats shell tells the user nothing about why it is
	// empty. One line, and closing and reopening tries again.
	const failedBlock = (
		<div className="text-body-secondary">Couldn't load. Try again.</div>
	);

	const modalHeader = nameBlock;
	const modalBody = failed ? (
		failedBlock
	) : (
		<>
			<RatingsStats
				ratings={ratings}
				stats={stats}
				type={type}
				tid={tid}
				coarseRatings={coarseRatings}
			/>
		</>
	);

	const popoverContent = (
		<div
			className="text-nowrap"
			style={{
				minWidth: 250,
			}}
		>
			<div className="mb-2">{nameBlock}</div>
			{failed ? (
				failedBlock
			) : (
				<RatingsStats
					ratings={ratings}
					stats={stats}
					type={type}
					tid={tid}
					coarseRatings={coarseRatings}
				/>
			)}
		</div>
	);

	const renderTarget = ({
		forwardedRef,
		onClick,
	}: {
		forwardedRef?: Ref<HTMLSpanElement>;
		onClick?: () => void;
	}) => <Icon ref={forwardedRef} onClick={onClick} watch={watch} />;

	return (
		<ResponsivePopover
			id={id}
			modalHeader={modalHeader}
			modalBody={modalBody}
			popoverContent={popoverContent}
			renderTarget={renderTarget}
			toggle={toggle}
		/>
	);
};
