import { svgsIndex } from "facesjs";
import { isSport } from "../../../common/sportFunctions.ts";
import { isUniformJersey, parseUniform } from "../../../common/uniform.ts";

const isValidJersey = (jersey: unknown) => {
	if (typeof jersey !== "string") {
		return false;
	}

	// A custom uniform spec, encoded in the jersey string. Basketball only for
	// now - the generated geometry is the basketball tank top.
	if (isSport("basketball") && isUniformJersey(jersey)) {
		return parseUniform(jersey) !== undefined;
	}

	// Make sure string is a valid jersey, regardless of sport
	if (isSport("baseball")) {
		const [jerseyId, accessoryId] = jersey.split(":");
		if (jerseyId === undefined || accessoryId === undefined) {
			return false;
		}

		if (
			!svgsIndex.jersey.includes(jerseyId as any) ||
			!svgsIndex.accessories.includes(accessoryId as any)
		) {
			return false;
		}
	} else {
		if (!svgsIndex.jersey.includes(jersey as any)) {
			return false;
		}
	}

	// Make sure sport matches
	return (
		(isSport("basketball") && jersey.startsWith("jersey")) ||
		(!isSport("basketball") && jersey.startsWith(process.env.SPORT))
	);
};

export default isValidJersey;
