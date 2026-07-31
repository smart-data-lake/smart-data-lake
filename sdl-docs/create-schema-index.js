const fs = require('fs');

const schemaDir = "static/json-schema-viewer/schemas/"
const availableSchemas = fs.readdirSync(schemaDir)
    .filter(f => f.endsWith(".json") && f.includes("-"));
if (availableSchemas.size === 0) console.error("no schemas found in", schemaDir);
// write to index.json file
fs.writeFileSync(schemaDir + "index.json", JSON.stringify(availableSchemas));
