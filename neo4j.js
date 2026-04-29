// https://www.kaggle.com/datasets/bfbarry/ucsd-course-prerequisites
// http://localhost:7474/browser/
const neo4j = require("neo4j-driver");
const fs = require("fs");
require("dotenv").config();

const prerequisiteFilePath = "./datasets/all_prerequisites.json";
const courseCodesFilePath = "./datasets/course_codes.json";

const driver = neo4j.driver(process.env.NEO_CONNECTION);
const session = driver.session();

async function createGraph(codesFilePath) {
	const session = driver.session();
	try {
		const courses = JSON.parse(
			fs.readFileSync(prerequisiteFilePath, "utf8"),
		);
		const subjectLookup = JSON.parse(
			fs.readFileSync(courseCodesFilePath, "utf8"),
		);

		for (const [code, info] of Object.entries(courses)) {
			const deptCode = code.split(" ");
			const deptName =
				subjectLookup[deptCode[0]] || "General Education";

			const courseName = info.name || "Untitled Course";
			const courseDesc =
				info.description || "No description provided.";

			// Create Course Node with properties
			await session.run(
				`
        MERGE (c:Course {code: $code})
        SET c.name = $name, 
            c.description = $description, 
            c.deptName = $deptName
      `,
				{
					code: code,
					name: courseName,
					description: courseDesc,
					deptName: deptName,
				},
			);

			// Create Relations
			if (
				info.prerequisites &&
				Array.isArray(info.prerequisites) &&
				info.prerequisites.length > 0
			) {
				const prereqs = info.prerequisites;

				for (let i = 0; i < prereqs.length; i++) {
					const pCode = prereqs[i];

					if (pCode === "or" || pCode === "and" || !pCode) {
						continue;
					}

					let relType = "Mandatory";
					const prev = i > 0 ? prereqs[i - 1] : null;
					const next =
						i < prereqs.length - 1 ? prereqs[i + 1] : null;

					if (prev === "or" || next === "or") {
						relType = "Optional";
					}

					await session.run(
						`
            MERGE (p:Course {code: $pCode})
            WITH p
            MATCH (target:Course {code: $targetCode})
            MERGE (p)-[:PREREQUISITE_FOR {type: $relType}]->(target)
            `,
						{ pCode, targetCode: code, relType },
					);
				}
			}
		}

		console.log(`==========> Graph was created`);
	} catch (err) {
		console.error(err.message);
	} finally {
		await session.close();
	}
}

// ====================================================================================
// ======================================2=============================================
// ====================================================================================

async function deleteNodeByCode(courseCode) {
	try {
		const result = await session.run(
			`
      MATCH (c:Course {code: $code})
      DETACH DELETE c
      `,
			{ code: courseCode },
		);
		console.log(
			`Node ${courseCode} and all its relationships were deleted.`,
		);
		return result;
	} catch (error) {
		console.error("Error deleting node:", error);
	}
}

async function deleteRelationship(sourceCode, targetCode) {
	try {
		const result = await session.run(
			`
      MATCH (source:Course {code: $sourceCode})-[r]->(target:Course {code: $targetCode})
      DELETE r
      `,
			{ sourceCode, targetCode },
		);
		console.log(
			`Relationship between ${sourceCode} and ${targetCode} deleted.`,
		);
		return result;
	} catch (error) {
		console.error("Error deleting relationship:", error);
	}
}

async function deleteDescriptionProperty(courseCode) {
	try {
		const result = await session.run(
			`
      MATCH (c:Course {code: $code})
      REMOVE c.description
      `,
			{ code: courseCode },
		);
		console.log(`Description property removed from ${courseCode}.`);
		return result;
	} catch (error) {
		console.error("Error removing property:", error);
	}
}

// ====================================================================================
// =======================================3============================================
// ====================================================================================
async function updateCourseDescription(courseCode, newDescription) {
	try {
		const result = await session.run(
			`
      MATCH (c:Course {code: $code})
      SET c.description = $newDescription
      RETURN c
      `,
			{ code: courseCode, newDescription: newDescription },
		);
		console.log(`Updated description for ${courseCode}.`);
		return result;
	} catch (error) {
		console.error("Error updating course description:", error);
	}
}

async function updatePrerequisiteType(sourceCode, targetCode, newType) {
	try {
		const result = await session.run(
			`
      MATCH (source:Course {code: $sourceCode})-[r:PREREQUISITE_FOR]->(target:Course {code: $targetCode})
      SET r.type = $newType
      RETURN r
      `,
			{ sourceCode, targetCode, newType },
		);
		console.log(
			`Updated prerequisite type between ${sourceCode} and ${targetCode} to ${newType}.`,
		);
		return result;
	} catch (error) {
		console.error("Error updating relationship property:", error);
	}
}

// ====================================================================================
// =======================================4============================================
// ====================================================================================
async function getCourseProperties(courseCode) {
	try {
		const result = await session.run(
			`
      MATCH (c:Course {code: $code})
      RETURN properties(c) AS courseProps
      `,
			{ code: courseCode },
		);

		// console.log(result)
		if (result.records.length === 0) {
			console.log(`No course found with the code: ${courseCode}`);
			return null;
		}

		const properties = result.records[0].get("courseProps");

		console.log(`Properties for ${courseCode}:`);
		for (const [key, value] of Object.entries(properties)) {
			console.log(`  - ${key}: ${value}`);
		}

		// return properties;
	} catch (error) {
		console.error("Error retrieving course:", error);
	}
}

// ====================================================================================
// =======================================5============================================
// ====================================================================================

async function getMandatoryPrerequisites(courseCode) {
	try {
		const result = await session.run(
			`
      MATCH (prereq:Course)-[:PREREQUISITE_FOR {type: "Mandatory"}]->(target:Course {code: $code})
      RETURN prereq.code AS prereqCode
      `,
			{ code: courseCode },
		);
		console.log(`Mandatory prerequisites for ${courseCode}:`);
		if (result.records.length === 0) {
			console.log("  - None");
		} else {
			result.records.forEach((record) => {
				console.log(`  - ${record.get("prereqCode")}`);
			});
		}
	} catch (error) {
		console.error("Error fetching mandatory prerequisites:", error);
	}
}

async function getOptionalPrerequisites(courseCode) {
	try {
		const result = await session.run(
			`
      MATCH (prereq:Course)-[:PREREQUISITE_FOR {type: "Optional"}]->(target:Course {code: $code})
      RETURN prereq.code AS prereqCode
      `,
			{ code: courseCode },
		);

		console.log(`Optional prerequisites for ${courseCode}:`);
		if (result.records.length === 0) {
			console.log("  - None");
		} else {
			result.records.forEach((record) => {
				console.log(`  - ${record.get("prereqCode")}`);
			});
		}
	} catch (error) {
		console.error("Error fetching optional prerequisites:", error);
	}
}

async function main() {
	try {
		// 1. Create the graph, including nodes, relationships, and properties.
		await createGraph();
		console.log("\n\n");

		// 2. Attempt to delete some nodes, relationships, and properties.
		await deleteRelationship("MATH 180A", "ECE 156");
		await deleteDescriptionProperty("MATH 180A");
		await deleteNodeByCode("MATH 180A");
		console.log("\n\n");

		// 3. Update the properties of some nodes and relationships.
		await updateCourseDescription("ECE 156", "TEST DESCRIPTION");
		await updatePrerequisiteType("MATH 186", "ECE 156", "Mandatory");
		console.log("\n\n");

		// 4. Find nodes based on any condition you specify.
		await getCourseProperties("ECE 156");
		console.log("\n\n");

		// 5. Find relationships based on any condition you specify.
		await getMandatoryPrerequisites("ECE 156");
		await getOptionalPrerequisites("ECE 156");
		console.log("\n\n");
	} catch (error) {
		console.error("Error during execution:", error);
	} finally {
		await driver.close();
	}
}

main();

/*
-- Get a specific course & its relations --

  MATCH (n:Course {code: "ECE 156"})-[r]-(m)
  RETURN n, r, m;

MATCH (n:Course {code: "MATH 180A"})-[r]-(m)
RETURN n, r, m;
*/
