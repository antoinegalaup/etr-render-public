import mysql from "mysql2/promise";
import {
  PUBLIC_PROPERTY_DEFINITIONS,
  buildPublicProperty
} from "./publicSiteCatalog.js";

function decodeHtml(value) {
  return `${value || ""}`
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&#8217;|&rsquo;/gi, "'")
    .replace(/&#8220;|&#8221;|&ldquo;|&rdquo;/gi, '"')
    .replace(/&#8211;|&#8212;|&ndash;|&mdash;/gi, "-")
    .replace(/&hellip;/gi, "...")
    .replace(/&quot;/gi, '"')
    .replace(/&#039;/gi, "'");
}

function cleanText(value) {
  return decodeHtml(
    `${value || ""}`
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<[^>]+>/g, " ")
  )
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function normalizeDisplayName(value) {
  const base = `${value || ""}`.split(":")[0]?.trim() || "";
  if (!base) {
    return "";
  }
  if (/^key\s*lime/i.test(base)) {
    return "KeyLime Cottage";
  }
  return base;
}

function summarizeInfo(value, fallback = "") {
  const cleaned = cleanText(value);
  if (!cleaned) {
    return fallback;
  }
  const paragraphs = cleaned
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean);
  return paragraphs.slice(0, 2).join(" ");
}

function splitImages(value) {
  return `${value || ""}`
    .split(";;")
    .map((part) => part.trim())
    .filter(Boolean);
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function flattenManifest(mediaManifest = {}) {
  return unique(
    ["hero", "editorial", "gallery", "detail", "drone", "fallback"].flatMap(
      (key) => mediaManifest[key] || []
    )
  );
}

function scenicScore(fileName) {
  const lower = `${fileName || ""}`.toLowerCase();
  let score = 0;
  if (/dji|drone/.test(lower)) score += 10;
  if (/beach|water|ocean|sea|shore/.test(lower)) score += 8;
  if (/pool|sunset|deck|porch|patio|stairs|gazebo/.test(lower)) score += 5;
  if (/front|entrance/.test(lower)) score += 2;
  if (/bathroom|bedroom|kitchen|living|dining/.test(lower)) score -= 3;
  return score;
}

function mergeMediaManifest(override, roomImages) {
  const overrideManifest = override?.media_manifest || {};
  const allFiles = unique([...flattenManifest(overrideManifest), ...roomImages]);
  const scenicFiles = allFiles
    .map((fileName) => ({ fileName, score: scenicScore(fileName) }))
    .sort((left, right) => right.score - left.score || left.fileName.localeCompare(right.fileName))
    .map((entry) => entry.fileName);
  const droneFiles = unique([
    ...(overrideManifest.drone || []),
    ...allFiles.filter((fileName) => /dji|drone/.test(fileName.toLowerCase()))
  ]);
  const detailFiles = unique([
    ...(overrideManifest.detail || []),
    ...allFiles.filter((fileName) =>
      /bathroom|detail|kitchen|living|dining|bedroom/.test(fileName.toLowerCase())
    )
  ]);
  const heroFiles = unique([
    ...(overrideManifest.hero || []),
    ...scenicFiles.filter((fileName) => !droneFiles.includes(fileName))
  ]).slice(0, 2);
  const editorialFiles = unique([
    ...(overrideManifest.editorial || []),
    ...scenicFiles.filter(
      (fileName) => !heroFiles.includes(fileName) && !droneFiles.includes(fileName)
    )
  ]).slice(0, 3);
  const galleryFiles = unique([
    ...(overrideManifest.gallery || []),
    ...allFiles.filter(
      (fileName) =>
        !heroFiles.includes(fileName) &&
        !editorialFiles.includes(fileName) &&
        !detailFiles.includes(fileName) &&
        !droneFiles.includes(fileName)
    )
  ]);
  const fallbackFiles = unique([
    ...(overrideManifest.fallback || []),
    roomImages[0],
    roomImages[1],
    allFiles[0]
  ]).slice(0, 2);

  return {
    hero: heroFiles.length ? heroFiles : fallbackFiles,
    editorial: editorialFiles,
    gallery: galleryFiles,
    detail: detailFiles,
    drone: droneFiles,
    fallback: fallbackFiles
  };
}

function parseRoomParams(value) {
  try {
    return JSON.parse(value || "{}");
  } catch {
    return {};
  }
}

function mergeBadges(overrideBadges, capacity, nightlyRate) {
  const badges = (overrideBadges || []).filter((badge) => !/^Sleeps\s+\d+/i.test(badge));
  if (capacity) {
    badges.unshift(`Sleeps ${capacity}`);
  }
  if (nightlyRate > 0) {
    badges.push(`From $${nightlyRate}/night`);
  }
  return unique(badges).slice(0, 4);
}

function mergeBookingRules(overrideRules = [], minimumStay = 1) {
  if (!overrideRules.length) {
    return [
      { label: "Minimum stay", detail: `${minimumStay} night${minimumStay === 1 ? "" : "s"}` },
      { label: "Check-in", detail: "After 4:00 PM" },
      { label: "Check-out", detail: "Before 10:00 AM" }
    ];
  }

  return overrideRules.map((rule) =>
    rule.label === "Minimum stay"
      ? {
          ...rule,
          detail: `${minimumStay} night${minimumStay === 1 ? "" : "s"}`
        }
      : rule
  );
}

function mergePriceSummary(summary = {}, nightlyRate = 0) {
  return {
    starting_from: nightlyRate || summary.starting_from || 0,
    seasonal_note: summary.seasonal_note || "Rates follow the active VikBooking pricing table.",
    stay_note: summary.stay_note || "Availability and pricing are read from the live booking system."
  };
}

function buildDefinitionFromRoom(room, pricingByRoomId, override) {
  const roomImages = unique([room.img, ...splitImages(room.moreimgs)]);
  const nightlyRate = Number(pricingByRoomId.get(room.id)?.nightly_rate || 0);
  const minimumStay = Number(pricingByRoomId.get(room.id)?.minimum_stay || 1) || 1;
  const params = parseRoomParams(room.params);
  const maxPeople = Number(room.totpeople || params.maxminpeople || override?.capacity || 0) || 0;
  const displayName = override?.name || normalizeDisplayName(room.name) || `${room.name || ""}`.trim();
  const shortDescription = cleanText(room.smalldesc) || override?.short_description || "";
  const longDescription =
    summarizeInfo(room.info, shortDescription || override?.long_description || "") ||
    override?.long_description ||
    shortDescription;

  return {
    ...override,
    room_id: room.id,
    slug: override?.slug || `${room.alias || displayName}`.trim().toLowerCase(),
    name: displayName,
    tagline: override?.tagline || shortDescription || displayName,
    short_description: shortDescription || override?.short_description || displayName,
    long_description: longDescription,
    capacity: maxPeople || override?.capacity || 0,
    base_price_hint: nightlyRate || override?.base_price_hint || 0,
    badges: mergeBadges(override?.badges, maxPeople || override?.capacity || 0, nightlyRate),
    booking_rules: mergeBookingRules(override?.booking_rules, minimumStay),
    price_summary: mergePriceSummary(override?.price_summary, nightlyRate),
    media_manifest: mergeMediaManifest(override, roomImages)
  };
}

async function queryRooms(connection, tablePrefix) {
  const [rows] = await connection.execute(
    `
      SELECT
        id,
        name,
        alias,
        smalldesc,
        info,
        totpeople,
        img,
        moreimgs,
        params
      FROM ${tablePrefix}vikbooking_rooms
      WHERE avail = 1
      ORDER BY id ASC
    `
  );
  return rows;
}

async function queryRoomPricing(connection, tablePrefix) {
  const [rows] = await connection.execute(
    `
      SELECT
        dc.idroom AS room_id,
        ROUND(MIN(dc.cost / GREATEST(dc.days, 1)), 2) AS nightly_rate,
        COALESCE(MIN(NULLIF(p.minlos, 0)), 1) AS minimum_stay
      FROM ${tablePrefix}vikbooking_dispcost dc
      LEFT JOIN ${tablePrefix}vikbooking_prices p
        ON p.id = dc.idprice
      GROUP BY dc.idroom
    `
  );
  return new Map(
    rows.map((row) => [
      Number(row.room_id),
      {
        nightly_rate: Number(row.nightly_rate || 0),
        minimum_stay: Number(row.minimum_stay || 1) || 1
      }
    ])
  );
}

function createConnectionOptions(options = {}) {
  const host = `${options.host || process.env.LOCAL_WP_DB_HOST || ""}`.trim();
  if (!host) {
    return null;
  }
  return {
    host,
    port: Number(options.port || process.env.LOCAL_WP_DB_PORT || 3307),
    user: `${options.user || process.env.LOCAL_WP_DB_USER || ""}`.trim(),
    password: `${options.password || process.env.LOCAL_WP_DB_PASSWORD || ""}`.trim(),
    database: `${options.database || process.env.LOCAL_WP_DB_NAME || ""}`.trim(),
    tablePrefix: `${options.tablePrefix || process.env.LOCAL_WP_TABLE_PREFIX || "wp_"}`.trim()
  };
}

export async function getLiveVikBookingProperties({
  wordpressBaseUrl = "",
  host,
  port,
  user,
  password,
  database,
  tablePrefix
} = {}) {
  const connectionOptions = createConnectionOptions({
    host,
    port,
    user,
    password,
    database,
    tablePrefix
  });
  if (!connectionOptions?.host || !connectionOptions.user || !connectionOptions.database) {
    return null;
  }

  const connection = await mysql.createConnection({
    host: connectionOptions.host,
    port: connectionOptions.port,
    user: connectionOptions.user,
    password: connectionOptions.password,
    database: connectionOptions.database
  });

  try {
    const [rooms, pricingByRoomId] = await Promise.all([
      queryRooms(connection, connectionOptions.tablePrefix),
      queryRoomPricing(connection, connectionOptions.tablePrefix)
    ]);
    const overridesByRoomId = new Map(
      PUBLIC_PROPERTY_DEFINITIONS.map((definition) => [Number(definition.room_id), definition])
    );

    return rooms.map((room) =>
      buildPublicProperty(
        buildDefinitionFromRoom(
          room,
          pricingByRoomId,
          overridesByRoomId.get(Number(room.id)) || null
        ),
        wordpressBaseUrl
      )
    );
  } finally {
    await connection.end();
  }
}

export async function getLiveVikBookingAvailability(
  roomIds,
  {
    host,
    port,
    user,
    password,
    database,
    tablePrefix,
    checkinUnix,
    checkoutUnix
  } = {}
) {
  const normalizedRoomIds = unique((roomIds || []).map((value) => Number(value)).filter(Boolean));
  if (!normalizedRoomIds.length || !checkinUnix || !checkoutUnix || checkinUnix >= checkoutUnix) {
    return null;
  }

  const connectionOptions = createConnectionOptions({
    host,
    port,
    user,
    password,
    database,
    tablePrefix
  });
  if (!connectionOptions?.host || !connectionOptions.user || !connectionOptions.database) {
    return null;
  }

  const connection = await mysql.createConnection({
    host: connectionOptions.host,
    port: connectionOptions.port,
    user: connectionOptions.user,
    password: connectionOptions.password,
    database: connectionOptions.database
  });

  try {
    const placeholders = normalizedRoomIds.map(() => "?").join(", ");
    const [rows] = await connection.execute(
      `
        SELECT
          r.id AS room_id,
          r.name AS room_name,
          GREATEST(COALESCE(r.units, 1), 1) AS units,
          SUM(
            CASE
              WHEN b.id IS NOT NULL
                AND (o.id IS NULL OR COALESCE(o.status, '') NOT IN ('cancelled', 'closed'))
              THEN 1
              ELSE 0
            END
          ) AS overlap_count
        FROM ${connectionOptions.tablePrefix}vikbooking_rooms r
        LEFT JOIN ${connectionOptions.tablePrefix}vikbooking_busy b
          ON b.idroom = r.id
         AND b.checkin < ?
         AND b.checkout > ?
        LEFT JOIN ${connectionOptions.tablePrefix}vikbooking_ordersbusy ob
          ON ob.idbusy = b.id
        LEFT JOIN ${connectionOptions.tablePrefix}vikbooking_orders o
          ON o.id = ob.idorder
        WHERE r.id IN (${placeholders})
        GROUP BY r.id, r.name, r.units
        ORDER BY r.id ASC
      `,
      [checkoutUnix, checkinUnix, ...normalizedRoomIds]
    );

    return rows.map((row) => {
      const units = Number(row.units || 1) || 1;
      const overlapCount = Number(row.overlap_count || 0) || 0;
      return {
        room_id: Number(row.room_id),
        room_name: row.room_name || null,
        units,
        overlap_count: overlapCount,
        available: overlapCount < units
      };
    });
  } finally {
    await connection.end();
  }
}
