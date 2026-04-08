import fs from "node:fs";
import path from "node:path";

export function readMaybeJson(filePath) {
  if (!filePath) return null;
  const raw = fs.readFileSync(filePath, "utf8").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

export function writeJson(filePath, data) {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

export function uniqueStrings(values) {
  return Array.from(new Set(values.filter(Boolean).map((v) => `${v}`)));
}
