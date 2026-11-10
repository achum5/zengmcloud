import type {
	FlatContractInfo,
	PlayerWithoutKey,
} from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import { getContractSummary } from "../contract/index.ts";

export type ContractHolder = Pick<
	PlayerWithoutKey,
	"contractDemand" | "contracts" | "tid"
>;

// The flat contract a front office reads off a player: what he's asking for
// if he has a contract demand (free agents, prospects, unsigned draft picks,
// expiring contracts while re-signing), otherwise what he's paid under his
// current contract and the season it runs through. This is what p.contract
// was before p.contracts. A fresh object, so changing it changes nothing.
export const contractOf = (p: ContractHolder): FlatContractInfo => {
	return (
		getContractSummary(p) ?? {
			amount: g.get("minContract"),
			exp: g.get("season"),
		}
	);
};

// For helpers.justDrafted, which reads the draft year and whether his contract
// is a rookie contract
export const draftedAs = (
	p: ContractHolder & Pick<PlayerWithoutKey, "draft">,
) => ({
	draft: p.draft,
	contract: contractOf(p),
});
