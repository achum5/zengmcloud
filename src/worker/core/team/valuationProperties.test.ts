import { runValuationProperties } from "../../../test/fixtures/valuationProperties.shared.ts";

// The same invariants, under this sport's constants. They are not cosmetic
// differences: the exponent that turns a player's value into a trade value is
// 7 in basketball and 3 in football and baseball, and the premium for giving
// up a pile of picks divides by 5 rather than 2.5 - which is the exact
// constant the second-round pick defect lived in. Direction has to survive
// all of it.
runValuationProperties();
