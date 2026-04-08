function isHostedSupabaseHost(hostname = "") {
  const host = `${hostname || ""}`.trim().toLowerCase();
  return (
    host.endsWith(".supabase.co") ||
    host.endsWith(".pooler.supabase.com") ||
    host.includes(".pooler.supabase.com")
  );
}

export function buildPgConnectionOptions(databaseUrl) {
  const rawUrl = `${databaseUrl || ""}`.trim();
  if (!rawUrl) {
    return {};
  }

  try {
    const parsed = new URL(rawUrl);
    const sslMode = `${parsed.searchParams.get("sslmode") || ""}`.trim().toLowerCase();
    const shouldDisableSsl = sslMode === "disable";
    const shouldRelaxSsl =
      !shouldDisableSsl &&
      (sslMode === "require" ||
        sslMode === "verify-ca" ||
        sslMode === "verify-full" ||
        sslMode === "no-verify" ||
        isHostedSupabaseHost(parsed.hostname));

    if (shouldDisableSsl || shouldRelaxSsl) {
      parsed.searchParams.delete("sslmode");
      return {
        connectionString: parsed.toString(),
        ssl: shouldDisableSsl ? false : { rejectUnauthorized: false }
      };
    }
  } catch {
    return { connectionString: rawUrl };
  }

  return { connectionString: rawUrl };
}
