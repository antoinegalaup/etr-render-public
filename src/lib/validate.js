import fs from "node:fs";
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

export function loadSchema(schemaPath) {
  return JSON.parse(fs.readFileSync(schemaPath, "utf8"));
}

export function validateManifest(manifest, schema, strict = true) {
  const ajv = new Ajv2020({
    allErrors: true,
    strict
  });
  addFormats(ajv);
  const validate = ajv.compile(schema);
  const valid = validate(manifest);
  return {
    valid: Boolean(valid),
    errors: validate.errors || []
  };
}
