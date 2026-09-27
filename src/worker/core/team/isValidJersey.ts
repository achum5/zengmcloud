import { svgsIndex } from "facesjs";
import { isUniformJersey, parseUniform } from "../../../common/uniform.ts";

const isValidJersey = (jersey: unknown) => {
	if (typeof jersey !== "string") {
		return false;
	}

	// A custom uniform spec, encoded in the jersey string. Basketball only for
	// now - the generated geometry is the basketball tank top.
	if (__SPORT === "basketball" && isUniformJersey(jersey)) {
		return parseUniform(jersey) !== undefined;
	}

	// Make sure string is a valid jersey, regardless of sport
	if (__SPORT === "baseball") {
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
		(__SPORT === "basketball" && jersey.startsWith("jersey")) ||
		(__SPORT !== "basketball" && jersey.startsWith(__SPORT))
	);
};

export default isValidJersey;
