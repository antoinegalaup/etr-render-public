import { randomUUID } from "node:crypto";

const PROPERTY_OPTIONS = [
  {
    roomId: "1",
    name: "KL Cottage",
    aliases: ["kl", "kl cottage", "key lime cottage", "keylime cottage", "keylime"]
  },
  {
    roomId: "5",
    name: "Lake Cottage",
    aliases: ["lake cottage", "lake house", "oleander"]
  },
  {
    roomId: "6",
    name: "Villa Esencia",
    aliases: ["villa esencia", "villa", "esencia"]
  }
];

const DEFAULT_STREAM_WINDOW_DAYS = 14;
const MANAGER_SELECTABLE_AGENT_IDS = ["vincent", "gael", "customer_service"];
const EMPLOYEE_AGENT_IDS = ["vincent"];

function validateIdentifier(value, label) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`invalid_${label}:${value}`);
  }
  return value;
}

function quoteIdentifier(value) {
  return `"${validateIdentifier(value, "identifier").replace(/"/g, "\"\"")}"`;
}

function normalizeWindowInput(from, to) {
  const start = from ? new Date(from) : new Date();
  const end = to ? new Date(to) : new Date(start.getTime() + DEFAULT_STREAM_WINDOW_DAYS * 86400000);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || start >= end) {
    throw new Error("invalid_dashboard_window");
  }
  return {
    from: start.toISOString(),
    to: end.toISOString()
  };
}

function startOfDay(date = new Date()) {
  const next = new Date(date);
  next.setHours(0, 0, 0, 0);
  return next;
}

function addDays(date, days) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function addHours(date, hours) {
  const next = new Date(date);
  next.setHours(next.getHours() + hours);
  return next;
}

function roundToNextHour(date = new Date()) {
  const next = new Date(date);
  next.setMinutes(0, 0, 0);
  next.setHours(next.getHours() + 1);
  return next;
}

function trimText(value, fallback = "") {
  const normalized = `${value || ""}`.trim();
  return normalized || fallback;
}

function normalizeGaelReply(value) {
  const stripped = trimText(value)
    .replace(/\*\*(.*?)\*\*/g, "$1")
    .replace(/__(.*?)__/g, "$1")
    .replace(/^#{1,6}\s*/gm, "")
    .replace(/^[*•]\s+/gm, "- ")
    .trim();
  if (!stripped) {
    return "";
  }
  const lines = stripped
    .split(/\n+/)
    .map((line) => trimText(line))
    .filter(Boolean);
  if (!lines.length) {
    return "";
  }
  const normalizedLines = lines
    .map((line) => (/^(-|\d+\.)\s/.test(line) ? line : `- ${line}`))
    .filter((line, index, allLines) => {
      if (index === 0 && allLines.length > 1 && /^-\s+(based on|summary|recommendation|here)/i.test(line)) {
        return false;
      }
      return true;
    })
    .slice(0, 3)
    .map((line) => {
      const prefixMatch = line.match(/^(-|\d+\.)\s+/);
      const prefix = prefixMatch ? prefixMatch[0] : "- ";
      const body = trimText(line.replace(/^(-|\d+\.)\s+/, ""))
        .replace(/:+$/g, "")
        .split(/\s+/)
        .slice(0, 14)
        .join(" ");
      return `${prefix}${body}`;
    })
    .filter((line) => !/^(-|\d+\.)\s*$/.test(line));
  return normalizedLines.join("\n");
}

function buildActor(actor = {}, fallbackSource = "user") {
  const staffRole = trimText(actor.staffRole || actor.staff_role).toLowerCase() || null;
  return {
    userId: actor.userId || actor.user_id || null,
    email: trimText(actor.email || actor.user_email),
    source: trimText(actor.source, fallbackSource),
    staffRole,
    isElevatedStaff:
      typeof actor.isElevatedStaff === "boolean"
        ? actor.isElevatedStaff
        : Boolean(staffRole && staffRole !== "employee")
  };
}

function assertAgentThreadAccess(threadRow = {}, actor = {}) {
  const resolvedActor = buildActor(actor);
  if (resolvedActor.isElevatedStaff) {
    return resolvedActor;
  }
  if (
    resolvedActor.userId &&
    threadRow.created_by &&
    `${threadRow.created_by}` === `${resolvedActor.userId}`
  ) {
    return resolvedActor;
  }
  throw new Error("agent_thread_access_denied");
}

function assertAgentActionAccess(actionRow = {}, actor = {}) {
  const resolvedActor = buildActor(actor);
  if (resolvedActor.isElevatedStaff) {
    return resolvedActor;
  }
  if (
    resolvedActor.userId &&
    actionRow.created_by &&
    `${actionRow.created_by}` === `${resolvedActor.userId}`
  ) {
    return resolvedActor;
  }
  throw new Error("agent_action_access_denied");
}

function normalizeActionRow(row = {}) {
  return {
    id: row.id,
    thread_id: row.thread_id || null,
    action_type: row.action_type,
    target_system: row.target_system,
    status: row.status,
    summary: row.summary || "",
    command_payload: row.command_payload || {},
    result_payload: row.result_payload || {},
    created_at: row.created_at || null,
    updated_at: row.updated_at || null
  };
}

function normalizeMessageRow(row = {}, pendingActions = []) {
  return {
    id: row.id,
    thread_id: row.thread_id,
    role: row.role,
    content: row.content,
    created_at: row.created_at,
    pending_actions: pendingActions
  };
}

function normalizeAgentId(value) {
  const normalized = trimText(value, "vincent")
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  if (["gael", "claude", "claude_code"].includes(normalized)) {
    return "gael";
  }
  if (["customer_service", "guest_service", "support"].includes(normalized)) {
    return "customer_service";
  }
  if (["vincent", "tessa", "mira"].includes(normalized)) {
    return normalized;
  }
  return "vincent";
}

function normalizeInteractionMode(value) {
  const normalized = trimText(value, "text")
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  if (["voice_note", "voice_call", "text"].includes(normalized)) {
    return normalized;
  }
  return "text";
}

function allowedAgentIdsForActor(actor = {}) {
  const resolvedActor = buildActor(actor);
  return resolvedActor.isElevatedStaff ? MANAGER_SELECTABLE_AGENT_IDS : EMPLOYEE_AGENT_IDS;
}

function assertActorCanUseAgent(agentId, actor = {}) {
  const resolvedActor = buildActor(actor);
  if (allowedAgentIdsForActor(resolvedActor).includes(agentId)) {
    return resolvedActor;
  }
  throw new Error("agent_access_denied");
}

function normalizeStreamEvent(row = {}) {
  return {
    cursor: `${row.cursor ?? row.id ?? ""}`,
    type: row.type || row.event_type || "change",
    changed_domains: Array.isArray(row.changed_domains) ? row.changed_domains : [],
    recommended_window: row.recommended_window || null,
    created_at: row.created_at || null,
    payload: row.payload || {}
  };
}

function resolvePropertyLabel(value) {
  const normalized = `${value || ""}`.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  const match = PROPERTY_OPTIONS.find(
    (entry) =>
      entry.name.toLowerCase() === normalized ||
      entry.roomId === normalized ||
      entry.aliases.some((alias) => normalized.includes(alias))
  );
  return match?.name || null;
}

function parseTaskType(message) {
  const normalized = `${message || ""}`.toLowerCase();
  if (normalized.includes("clean")) return "cleaning";
  if (normalized.includes("inspect")) return "inspection";
  if (normalized.includes("maint")) return "maintenance";
  if (normalized.includes("arrival")) return "arrival";
  if (normalized.includes("departure")) return "departure";
  if (normalized.includes("guest")) return "guest_service";
  return "custom";
}

function parseTimeRange(message) {
  const normalized = `${message || ""}`;
  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/i);
  if (!match) {
    return null;
  }
  let hour = Number.parseInt(match[1], 10);
  const minute = Number.parseInt(match[2] || "0", 10) || 0;
  const meridiem = match[3].toLowerCase();
  if (meridiem === "pm" && hour < 12) {
    hour += 12;
  }
  if (meridiem === "am" && hour === 12) {
    hour = 0;
  }
  return { hour, minute };
}

function inferTaskSchedule(message) {
  const normalized = `${message || ""}`.toLowerCase();
  const baseDay = normalized.includes("tomorrow") ? addDays(startOfDay(), 1) : startOfDay();
  const parsedTime = parseTimeRange(normalized);
  const start = parsedTime
    ? new Date(
        baseDay.getFullYear(),
        baseDay.getMonth(),
        baseDay.getDate(),
        parsedTime.hour,
        parsedTime.minute,
        0,
        0
      )
    : normalized.includes("today") || normalized.includes("tomorrow")
      ? new Date(baseDay.getFullYear(), baseDay.getMonth(), baseDay.getDate(), 9, 0, 0, 0)
      : roundToNextHour();
  return {
    startAt: start.toISOString(),
    endAt: addHours(start, 1).toISOString()
  };
}

function extractQuotedTitle(message) {
  const match = `${message || ""}`.match(/"([^"]{3,120})"/);
  return match ? trimText(match[1]) : "";
}

function buildAgentTaskDraft(message) {
  const normalized = `${message || ""}`.toLowerCase();
  const requestsTask =
    /(add|create|schedule|queue)/.test(normalized) &&
    /(task|clean|inspection|maint|arrival|departure|guest)/.test(normalized);
  if (!requestsTask) {
    return null;
  }

  const taskType = parseTaskType(message);
  const propertyLabel = resolvePropertyLabel(message) || "Property TBD";
  const title =
    extractQuotedTitle(message) ||
    `${taskType === "custom" ? "Ops task" : taskType.replace(/_/g, " ")} for ${propertyLabel}`;
  const schedule = inferTaskSchedule(message);

  return {
    title,
    notes: trimText(message),
    task_type: taskType,
    status: "scheduled",
    priority: normalized.includes("urgent") ? "urgent" : normalized.includes("high") ? "high" : "normal",
    property_label: propertyLabel,
    start_at: schedule.startAt,
    end_at: schedule.endAt,
    reservation_uuid: null,
    assignee_ids: []
  };
}

function looksLikeGaelPlanningRequest(message) {
  const normalized = `${message || ""}`.toLowerCase();
  const peopleIntent =
    /(add|create|hire|bring in|onboard|set up|assign|update|change|edit|rename|activate|deactivate|remove)/.test(normalized) &&
    /(employee|manager|staff|person|people|team)/.test(normalized);
  const taskIntent =
    /(add|create|schedule|assign|put|turn|convert|make|plan|queue|update|change|move|reschedule|cancel|complete|finish|mark|edit|rename)/.test(normalized) &&
    /(task|tasks|calendar|schedule|shift|checklist|assignment|assignments|prep|arrival|departure)/.test(normalized);
  return peopleIntent || taskIntent;
}

function extractJsonObject(text) {
  const normalized = trimText(text);
  if (!normalized) {
    return null;
  }

  const fenced = normalized.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = fenced ? trimText(fenced[1]) : normalized;
  try {
    return JSON.parse(candidate);
  } catch {}

  const firstBrace = candidate.indexOf("{");
  const lastBrace = candidate.lastIndexOf("}");
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    try {
      return JSON.parse(candidate.slice(firstBrace, lastBrace + 1));
    } catch {}
  }
  return null;
}

function normalizePlannedRole(value) {
  const normalized = trimText(value).toLowerCase();
  if (!normalized) {
    return "";
  }
  if (["manager", "managers", "admin", "ops_manager", "operations_manager"].includes(normalized)) {
    return "Manager";
  }
  if (["employee", "employees", "staff", "staff_member", "team_member"].includes(normalized)) {
    return "Employee";
  }
  return "";
}

function defaultAccentColorForRole(role) {
  return `${role || ""}`.trim().toLowerCase() === "manager" ? "#0f766e" : "#1f6feb";
}

function normalizePlannedPerson(input = {}) {
  const fullName = trimText(input.full_name || input.fullName);
  const role = normalizePlannedRole(input.role);
  if (!fullName || !role) {
    return null;
  }
  return {
    full_name: fullName,
    role,
    phone: trimText(input.phone) || null,
    email: trimText(input.email).toLowerCase() || null,
    accent_color: trimText(input.accent_color || input.accentColor, defaultAccentColorForRole(role)),
    notes: trimText(input.notes) || null,
    is_active:
      typeof input.is_active === "boolean"
        ? input.is_active
        : typeof input.isActive === "boolean"
          ? input.isActive
          : true
  };
}

function normalizePlannedTask(input = {}, requestedSummary = "") {
  const rawTitle = trimText(input.title);
  if (!rawTitle) {
    return null;
  }
  const propertyLabel =
    trimText(input.property_label || input.propertyLabel) ||
    resolvePropertyLabel(requestedSummary) ||
    "Property TBD";
  let startAt = trimText(input.start_at || input.startAt);
  let endAt = trimText(input.end_at || input.endAt);
  if (!startAt || Number.isNaN(Date.parse(startAt)) || !endAt || Number.isNaN(Date.parse(endAt))) {
    const fallbackSchedule = inferTaskSchedule(`${requestedSummary} ${rawTitle}`);
    startAt = fallbackSchedule.startAt;
    endAt = fallbackSchedule.endAt;
  }
  const taskType = trimText(input.task_type || input.taskType, parseTaskType(`${rawTitle} ${requestedSummary}`));
  const priority = trimText(
    input.priority,
    `${requestedSummary}`.toLowerCase().includes("urgent")
      ? "urgent"
      : `${requestedSummary}`.toLowerCase().includes("high")
        ? "high"
        : "normal"
  );
  const assigneeNames = Array.isArray(input.assignee_names || input.assigneeNames)
    ? Array.from(new Set((input.assignee_names || input.assigneeNames).map((entry) => trimText(entry)).filter(Boolean)))
    : [];
  const assigneeEmails = Array.isArray(input.assignee_emails || input.assigneeEmails)
    ? Array.from(
        new Set(
          (input.assignee_emails || input.assigneeEmails)
            .map((entry) => trimText(entry).toLowerCase())
            .filter(Boolean)
        )
      )
    : [];

  return {
    title: rawTitle,
    notes: trimText(input.notes || requestedSummary) || null,
    task_type: taskType,
    status: "scheduled",
    priority,
    property_label: propertyLabel,
    start_at: new Date(startAt).toISOString(),
    end_at: new Date(endAt).toISOString(),
    reservation_uuid: trimText(input.reservation_uuid || input.reservationUUID) || null,
    assignee_ids: Array.isArray(input.assignee_ids || input.assigneeIDs)
      ? Array.from(new Set((input.assignee_ids || input.assigneeIDs).filter(Boolean)))
      : [],
    assignee_names: assigneeNames,
    assignee_emails: assigneeEmails
  };
}

function normalizeGaelPlan(rawPlan = {}, requestedSummary = "") {
  if (!rawPlan || typeof rawPlan !== "object" || Array.isArray(rawPlan)) {
    return null;
  }
  const people = Array.isArray(rawPlan.people)
    ? rawPlan.people.map((entry) => normalizePlannedPerson(entry)).filter(Boolean)
    : [];
  const tasks = Array.isArray(rawPlan.tasks)
    ? rawPlan.tasks.map((entry) => normalizePlannedTask(entry, requestedSummary)).filter(Boolean)
    : [];
  const peopleUpdates = Array.isArray(rawPlan.people_updates || rawPlan.peopleUpdates)
    ? (rawPlan.people_updates || rawPlan.peopleUpdates)
        .map((entry) => normalizePlannedPersonUpdate(entry))
        .filter(Boolean)
    : [];
  const taskUpdates = Array.isArray(rawPlan.task_updates || rawPlan.taskUpdates)
    ? (rawPlan.task_updates || rawPlan.taskUpdates)
        .map((entry) => normalizePlannedTaskUpdate(entry, requestedSummary))
        .filter(Boolean)
    : [];
  const reply = trimText(rawPlan.reply);
  const needsFollowUp = Boolean(rawPlan.needs_follow_up || rawPlan.needsFollowUp);
  const followUpQuestions = Array.isArray(rawPlan.follow_up_questions || rawPlan.followUpQuestions)
    ? (rawPlan.follow_up_questions || rawPlan.followUpQuestions)
        .map((entry) => trimText(entry))
        .filter(Boolean)
        .slice(0, 1)
    : [];
  return {
    reply,
    people,
    tasks,
    people_updates: peopleUpdates,
    task_updates: taskUpdates,
    needsFollowUp,
    followUpQuestions
  };
}

function buildGaelPlanSummary(plan) {
  const peopleCount = Array.isArray(plan.people) ? plan.people.length : 0;
  const taskCount = Array.isArray(plan.tasks) ? plan.tasks.length : 0;
  const peopleUpdatesCount = Array.isArray(plan.people_updates) ? plan.people_updates.length : 0;
  const taskUpdatesCount = Array.isArray(plan.task_updates) ? plan.task_updates.length : 0;
  const parts = [];
  if (peopleCount) {
    parts.push(`add ${peopleCount} ${peopleCount === 1 ? "person" : "people"}`);
  }
  if (taskCount) {
    parts.push(`schedule ${taskCount} ${taskCount === 1 ? "task" : "tasks"}`);
  }
  if (peopleUpdatesCount) {
    parts.push(`update ${peopleUpdatesCount} ${peopleUpdatesCount === 1 ? "person" : "people"}`);
  }
  if (taskUpdatesCount) {
    parts.push(`update ${taskUpdatesCount} ${taskUpdatesCount === 1 ? "task" : "tasks"}`);
  }
  return parts.join(" and ");
}

function hasGaelPlanChanges(plan = {}) {
  return Boolean(
    (Array.isArray(plan.people) && plan.people.length) ||
      (Array.isArray(plan.tasks) && plan.tasks.length) ||
      (Array.isArray(plan.people_updates) && plan.people_updates.length) ||
      (Array.isArray(plan.task_updates) && plan.task_updates.length)
  );
}

function inferMissingPersonQuestion(message = "") {
  const normalized = `${message || ""}`.toLowerCase();
  if (normalized.includes("manager")) {
    return "Who should I add as the manager?";
  }
  if (/(employee|staff|person|people|team)/.test(normalized)) {
    return "Who should I add as the employee?";
  }
  return "Who should I add?";
}

function buildSingleFollowUpReply(question, preparedLabel = "") {
  const lines = [];
  const normalizedPreparedLabel = trimText(preparedLabel);
  if (normalizedPreparedLabel) {
    lines.push(`- ${normalizedPreparedLabel}`);
  }
  if (trimText(question)) {
    lines.push(`- ${trimText(question).replace(/^[*-]\s*/, "").replace(/\?*$/, "?")}`);
  }
  return lines.join("\n");
}

function normalizePlannedPersonPatch(input = {}) {
  const patch = {};
  if ("full_name" in input || "fullName" in input) {
    const fullName = trimText(input.full_name || input.fullName);
    if (fullName) {
      patch.full_name = fullName;
    }
  }
  if ("role" in input) {
    const role = normalizePlannedRole(input.role) || trimText(input.role);
    if (role) {
      patch.role = role;
    }
  }
  if ("phone" in input) {
    patch.phone = trimText(input.phone) || null;
  }
  if ("email" in input) {
    patch.email = trimText(input.email).toLowerCase() || null;
  }
  if ("accent_color" in input || "accentColor" in input) {
    patch.accent_color = trimText(input.accent_color || input.accentColor) || null;
  }
  if ("notes" in input) {
    patch.notes = trimText(input.notes) || null;
  }
  if ("is_active" in input) {
    patch.is_active = Boolean(input.is_active);
  }
  if ("isActive" in input) {
    patch.is_active = Boolean(input.isActive);
  }
  return Object.keys(patch).length ? patch : null;
}

function normalizePlannedPersonUpdate(input = {}) {
  const matchInput = input.match && typeof input.match === "object" ? input.match : input;
  const patchInput =
    input.patch && typeof input.patch === "object"
      ? input.patch
      : input.updates && typeof input.updates === "object"
        ? input.updates
        : {};
  const match = {
    id: trimText(matchInput.id || matchInput.person_id || matchInput.personId) || null,
    full_name: trimText(matchInput.full_name || matchInput.fullName) || null,
    email: trimText(matchInput.email).toLowerCase() || null
  };
  const patch = normalizePlannedPersonPatch(patchInput);
  if (!patch) {
    return null;
  }
  if (!match.id && !match.full_name && !match.email) {
    return null;
  }
  return { match, patch };
}

function normalizePlannedTaskPatch(input = {}, requestedSummary = "") {
  const patch = {};
  if ("title" in input) {
    const title = trimText(input.title);
    if (title) {
      patch.title = title;
    }
  }
  if ("notes" in input) {
    patch.notes = trimText(input.notes) || null;
  }
  if ("task_type" in input || "taskType" in input) {
    const taskType = trimText(input.task_type || input.taskType);
    if (taskType) {
      patch.task_type = taskType;
    }
  }
  if ("status" in input) {
    const status = trimText(input.status);
    if (status) {
      patch.status = status;
    }
  }
  if ("priority" in input) {
    const priority = trimText(input.priority);
    if (priority) {
      patch.priority = priority;
    }
  }
  if ("start_at" in input || "startAt" in input) {
    const startAt = trimText(input.start_at || input.startAt);
    if (startAt && !Number.isNaN(Date.parse(startAt))) {
      patch.start_at = new Date(startAt).toISOString();
    }
  }
  if ("end_at" in input || "endAt" in input) {
    const endAt = trimText(input.end_at || input.endAt);
    if (endAt && !Number.isNaN(Date.parse(endAt))) {
      patch.end_at = new Date(endAt).toISOString();
    }
  }
  if ("property_label" in input || "propertyLabel" in input) {
    const propertyLabel =
      trimText(input.property_label || input.propertyLabel) || resolvePropertyLabel(requestedSummary);
    if (propertyLabel) {
      patch.property_label = propertyLabel;
    }
  }
  if ("reservation_uuid" in input || "reservationUUID" in input) {
    patch.reservation_uuid = trimText(input.reservation_uuid || input.reservationUUID) || null;
  }
  if ("assignee_ids" in input || "assigneeIDs" in input) {
    patch.assignee_ids = Array.isArray(input.assignee_ids || input.assigneeIDs)
      ? Array.from(new Set((input.assignee_ids || input.assigneeIDs).filter(Boolean)))
      : [];
  }
  if ("assignee_names" in input || "assigneeNames" in input) {
    patch.assignee_names = Array.isArray(input.assignee_names || input.assigneeNames)
      ? Array.from(new Set((input.assignee_names || input.assigneeNames).map((entry) => trimText(entry)).filter(Boolean)))
      : [];
  }
  if ("assignee_emails" in input || "assigneeEmails" in input) {
    patch.assignee_emails = Array.isArray(input.assignee_emails || input.assigneeEmails)
      ? Array.from(
          new Set(
            (input.assignee_emails || input.assigneeEmails)
              .map((entry) => trimText(entry).toLowerCase())
              .filter(Boolean)
          )
        )
      : [];
  }
  return Object.keys(patch).length ? patch : null;
}

function normalizePlannedTaskUpdate(input = {}, requestedSummary = "") {
  const matchInput = input.match && typeof input.match === "object" ? input.match : input;
  const patchInput =
    input.patch && typeof input.patch === "object"
      ? input.patch
      : input.updates && typeof input.updates === "object"
        ? input.updates
        : {};
  const match = {
    id: trimText(matchInput.id || matchInput.task_id || matchInput.taskId) || null,
    title: trimText(matchInput.title) || null,
    property_label:
      trimText(matchInput.property_label || matchInput.propertyLabel) || resolvePropertyLabel(matchInput.title || requestedSummary) || null,
    start_at: trimText(matchInput.start_at || matchInput.startAt) || null
  };
  const patch = normalizePlannedTaskPatch(patchInput, requestedSummary);
  if (!patch) {
    return null;
  }
  if (!match.id && !match.title && !match.property_label && !match.start_at) {
    return null;
  }
  return { match, patch };
}

function hasNamedStaffHints(message = "") {
  const propertyNames = new Set(
    PROPERTY_OPTIONS.flatMap((entry) => [entry.name, ...(entry.aliases || [])]).map((value) =>
      trimText(value).toLowerCase()
    )
  );
  const ignoredTokens = new Set([
    "today",
    "tomorrow",
    "morning",
    "afternoon",
    "evening",
    "arrival",
    "departure"
  ]);
  const matches = `${message || ""}`.match(/\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b/g) || [];
  return matches.some((entry) => {
    const normalized = trimText(entry).toLowerCase();
    return normalized && !ignoredTokens.has(normalized) && !propertyNames.has(normalized);
  });
}

function buildGaelPlanAction(plan, threadId, actor = {}) {
  const summary = buildGaelPlanSummary(plan);
  return {
    id: randomUUID(),
    thread_id: threadId,
    action_type: "ops.plan.apply",
    target_system: "ops",
    status: "pending_approval",
    summary: summary ? `Apply Gael plan: ${summary}` : "Apply Gael operations plan",
    command_payload: {
      requested_summary: trimText(plan.requestedSummary),
      people: plan.people,
      tasks: plan.tasks,
      people_updates: plan.people_updates,
      task_updates: plan.task_updates
    },
    created_by: actor.userId || null
  };
}

function normalizePersonMatchKey(value) {
  return trimText(value).toLowerCase();
}

export class StaffOperationsService {
  constructor(options = {}) {
    this.controlPlaneService = options.controlPlaneService || null;
    this.vincentService = options.vincentService || null;
    this.gaelService = options.gaelService || null;
    this.elevenLabsService = options.elevenLabsService || null;
    this.syncSchema = validateIdentifier(options.syncSchema || "sync", "sync_schema");
    this.opsSchema = validateIdentifier(options.opsSchema || "ops", "ops_schema");
  }

  async ensureReady() {
    if (this.controlPlaneService?.connect) {
      await this.controlPlaneService.connect();
    }
    if (!this.controlPlaneService?._client) {
      throw new Error("staff_service_unavailable");
    }
  }

  get client() {
    return this.controlPlaneService?._client;
  }

  async loadRecentThreadMessages(threadId, limit = 10) {
    if (!this.client || !threadId) {
      return [];
    }

    const opsSchema = quoteIdentifier(this.opsSchema);
    const response = await this.client.query(
      `
        SELECT role, content
        FROM ${opsSchema}.agent_messages
        WHERE thread_id = $1::uuid
          AND role IN ('user', 'assistant')
        ORDER BY created_at DESC
        LIMIT $2
      `,
      [threadId, limit]
    );

    return response.rows
      .slice()
      .reverse()
      .map((row) => ({
        role: row.role === "assistant" ? "assistant" : "user",
        content: trimText(row.content)
      }))
      .filter((row) => row.content);
  }

  async getDashboard({ from, to }) {
    await this.ensureReady();
    const window = normalizeWindowInput(from, to);
    const syncSchema = quoteIdentifier(this.syncSchema);
    const opsSchema = quoteIdentifier(this.opsSchema);

    const reservationsResponse = await this.client.query(
      `
        SELECT
          r.supabase_uuid,
          r.vikbooking_id,
          r.status,
          r.total,
          r.total_paid,
          r.customer_vikbooking_id,
          r.checkin_at,
          r.checkout_at,
          r.source_updated_at
        FROM ${syncSchema}.reservations r
        WHERE r.checkin_at <= $2::timestamptz
          AND r.checkout_at >= $1::timestamptz
        ORDER BY r.checkin_at ASC NULLS LAST, r.checkout_at ASC NULLS LAST
      `,
      [window.from, window.to]
    );
    const reservations = reservationsResponse.rows;
    const reservationIds = reservations.map((row) => row.supabase_uuid);
    const customerIds = Array.from(
      new Set(reservations.map((row) => row.customer_vikbooking_id).filter(Boolean))
    );

    const [customersResponse, reservationRoomsResponse, peopleResponse, tasksResponse, cursorResponse] =
      await Promise.all([
        customerIds.length
          ? this.client.query(
              `
                SELECT
                  supabase_uuid,
                  vikbooking_id,
                  email,
                  first_name,
                  last_name,
                  phone,
                  source_updated_at
                FROM ${syncSchema}.customers
                WHERE vikbooking_id = ANY($1::text[])
                ORDER BY last_name ASC NULLS LAST, first_name ASC NULLS LAST
              `,
              [customerIds]
            )
          : Promise.resolve({ rows: [] }),
        reservationIds.length
          ? this.client.query(
              `
                SELECT
                  reservation_supabase_uuid,
                  vikbooking_room_link_id,
                  vikbooking_room_id,
                  adults,
                  children,
                  room_cost,
                  source_updated_at
                FROM ${syncSchema}.reservation_rooms
                WHERE reservation_supabase_uuid = ANY($1::uuid[])
                ORDER BY reservation_supabase_uuid ASC, vikbooking_room_link_id ASC
              `,
              [reservationIds]
            )
          : Promise.resolve({ rows: [] }),
        this.client.query(
          `
            SELECT
              id,
              full_name,
              role,
              phone,
              email,
              accent_color,
              notes,
              is_active
            FROM ${opsSchema}.people
            WHERE is_active = TRUE
            ORDER BY full_name ASC
          `
        ),
        this.client.query(
          `
            SELECT
              id,
              title,
              notes,
              task_type,
              status,
              priority,
              start_at,
              end_at,
              property_label,
              reservation_uuid,
              created_at,
              updated_at
            FROM ${opsSchema}.calendar_tasks
            WHERE start_at <= $2::timestamptz
              AND end_at >= $1::timestamptz
            ORDER BY start_at ASC, end_at ASC
          `,
          [window.from, window.to]
        ),
        this.client.query(
          `
            SELECT COALESCE(MAX(id), 0)::text AS cursor
            FROM ${opsSchema}.change_events
          `
        )
      ]);

    const taskIds = tasksResponse.rows.map((row) => row.id);
    const assignmentsResponse = taskIds.length
      ? await this.client.query(
          `
            SELECT task_id, person_id, assigned_at
            FROM ${opsSchema}.task_assignees
            WHERE task_id = ANY($1::uuid[])
            ORDER BY task_id ASC, assigned_at ASC
          `,
          [taskIds]
        )
      : { rows: [] };

    return {
      window,
      reservations,
      customers: customersResponse.rows,
      reservation_rooms: reservationRoomsResponse.rows,
      people: peopleResponse.rows,
      tasks: tasksResponse.rows,
      assignments: assignmentsResponse.rows,
      server_cursor: cursorResponse.rows[0]?.cursor || "0"
    };
  }

  async listPeople() {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const response = await this.client.query(
      `
        SELECT
          id,
          full_name,
          role,
          phone,
          email,
          accent_color,
          notes,
          is_active
        FROM ${opsSchema}.people
        ORDER BY is_active DESC, full_name ASC
      `
    );
    return response.rows;
  }

  async listPeopleForAssignment(options = {}) {
    if (!options.skipReadyCheck) {
      await this.ensureReady();
    }
    const opsSchema = quoteIdentifier(this.opsSchema);
    const response = await this.client.query(
      `
        SELECT
          id,
          full_name,
          email,
          role,
          is_active
        FROM ${opsSchema}.people
        ORDER BY is_active DESC, full_name ASC
      `
    );
    return response.rows;
  }

  resolveAssigneeIds(taskInput = {}, people = []) {
    const explicitIds = Array.isArray(taskInput.assignee_ids || taskInput.assigneeIDs)
      ? Array.from(new Set((taskInput.assignee_ids || taskInput.assigneeIDs).filter(Boolean)))
      : [];
    if (explicitIds.length) {
      return explicitIds;
    }

    const availablePeople = Array.isArray(people) ? people : [];
    const byName = new Map();
    const byEmail = new Map();
    for (const person of availablePeople) {
      const nameKey = normalizePersonMatchKey(person.full_name || person.fullName);
      const emailKey = normalizePersonMatchKey(person.email);
      if (nameKey && !byName.has(nameKey)) {
        byName.set(nameKey, person.id);
      }
      if (emailKey && !byEmail.has(emailKey)) {
        byEmail.set(emailKey, person.id);
      }
    }

    const nameIds = Array.isArray(taskInput.assignee_names || taskInput.assigneeNames)
      ? (taskInput.assignee_names || taskInput.assigneeNames)
          .map((value) => byName.get(normalizePersonMatchKey(value)))
          .filter(Boolean)
      : [];
    const emailIds = Array.isArray(taskInput.assignee_emails || taskInput.assigneeEmails)
      ? (taskInput.assignee_emails || taskInput.assigneeEmails)
          .map((value) => byEmail.get(normalizePersonMatchKey(value)))
          .filter(Boolean)
      : [];
    return Array.from(new Set([...nameIds, ...emailIds]));
  }

  async findPersonForPlannedUpdate(match = {}, options = {}) {
    if (!options.skipReadyCheck) {
      await this.ensureReady();
    }
    const opsSchema = quoteIdentifier(this.opsSchema);
    const id = trimText(match.id);
    const email = trimText(match.email).toLowerCase();
    const fullName = trimText(match.full_name || match.fullName);

    if (!id && !email && !fullName) {
      throw new Error("person_update_target_missing");
    }

    if (id) {
      const response = await this.client.query(
        `
          SELECT id, full_name, email, role, is_active
          FROM ${opsSchema}.people
          WHERE id = $1::uuid
          LIMIT 1
        `,
        [id]
      );
      if (!response.rows.length) {
        throw new Error("person_update_target_not_found");
      }
      return response.rows[0];
    }

    const clauses = [];
    const values = [];
    if (email) {
      values.push(email);
      clauses.push(`LOWER(email) = $${values.length}`);
    }
    if (fullName) {
      values.push(fullName.toLowerCase());
      clauses.push(`LOWER(full_name) = $${values.length}`);
    }
    const response = await this.client.query(
      `
        SELECT id, full_name, email, role, is_active
        FROM ${opsSchema}.people
        WHERE ${clauses.join(" AND ")}
        ORDER BY is_active DESC, full_name ASC
        LIMIT 2
      `,
      values
    );
    if (!response.rows.length) {
      throw new Error("person_update_target_not_found");
    }
    if (response.rows.length > 1) {
      throw new Error("person_update_target_ambiguous");
    }
    return response.rows[0];
  }

  async findTaskForPlannedUpdate(match = {}, options = {}) {
    if (!options.skipReadyCheck) {
      await this.ensureReady();
    }
    const opsSchema = quoteIdentifier(this.opsSchema);
    const id = trimText(match.id);
    const title = trimText(match.title);
    const propertyLabel = trimText(match.property_label || match.propertyLabel);
    const startAt = trimText(match.start_at || match.startAt);

    if (!id && !title && !propertyLabel && !startAt) {
      throw new Error("task_update_target_missing");
    }

    if (id) {
      const response = await this.client.query(
        `
          SELECT
            id,
            title,
            property_label,
            start_at,
            end_at,
            status,
            priority
          FROM ${opsSchema}.calendar_tasks
          WHERE id = $1::uuid
          LIMIT 1
        `,
        [id]
      );
      if (!response.rows.length) {
        throw new Error("task_update_target_not_found");
      }
      return response.rows[0];
    }

    const clauses = [];
    const values = [];
    if (title) {
      values.push(title.toLowerCase());
      clauses.push(`LOWER(title) = $${values.length}`);
    }
    if (propertyLabel) {
      values.push(propertyLabel.toLowerCase());
      clauses.push(`LOWER(property_label) = $${values.length}`);
    }
    if (startAt && !Number.isNaN(Date.parse(startAt))) {
      values.push(new Date(startAt).toISOString());
      clauses.push(`start_at = $${values.length}::timestamptz`);
    }

    const response = await this.client.query(
      `
        SELECT
          id,
          title,
          property_label,
          start_at,
          end_at,
          status,
          priority
        FROM ${opsSchema}.calendar_tasks
        WHERE ${clauses.join(" AND ")}
        ORDER BY start_at ASC, created_at ASC
        LIMIT 2
      `,
      values
    );
    if (!response.rows.length) {
      throw new Error("task_update_target_not_found");
    }
    if (response.rows.length > 1) {
      throw new Error("task_update_target_ambiguous");
    }
    return response.rows[0];
  }

  async createPerson(input = {}, actor = {}) {
    await this.ensureReady();
    await this.client.query("BEGIN");
    try {
      const person = await this._createPersonWithinTransaction(input, actor, {
        source: actor.source || "user"
      });
      await this.client.query("COMMIT");
      return person;
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
  }

  async _createPersonWithinTransaction(input = {}, actor = {}, options = {}) {
    const resolvedActor = buildActor(actor, options.source || "user");
    const fullName = trimText(input.full_name || input.fullName);
    const role = trimText(input.role);
    const phone = trimText(input.phone) || null;
    const email = trimText(input.email).toLowerCase() || null;
    const notes = trimText(input.notes) || null;
    const accentColor = trimText(
      input.accent_color || input.accentColor,
      defaultAccentColorForRole(role)
    );
    const isActive =
      typeof input.is_active === "boolean"
        ? input.is_active
        : typeof input.isActive === "boolean"
          ? input.isActive
          : true;

    if (!fullName || !role) {
      throw new Error("person_validation_failed");
    }

    const opsSchema = quoteIdentifier(this.opsSchema);
    const response = await this.client.query(
      `
        INSERT INTO ${opsSchema}.people
          (
            id,
            full_name,
            role,
            phone,
            email,
            accent_color,
            notes,
            is_active
          )
        VALUES
          (
            $1::uuid,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8::boolean
          )
        RETURNING
          id,
          full_name,
          role,
          phone,
          email,
          accent_color,
          notes,
          is_active
      `,
      [randomUUID(), fullName, role, phone, email, accentColor, notes, isActive]
    );
    const person = response.rows[0];

    await this.recordAuditLog(
      {
        actor: resolvedActor,
        eventType: "person.created",
        entityType: "person",
        entityId: person.id,
        details: {
          role: person.role,
          is_active: person.is_active
        }
      },
      { skipReadyCheck: true }
    );
    await this.emitChangeEvent(
      {
        type: "person.created",
        changedDomains: ["people"],
        payload: {
          person_id: person.id
        }
      },
      { skipReadyCheck: true }
    );

    return person;
  }

  async createTask(input = {}, actor = {}) {
    await this.ensureReady();
    await this.client.query("BEGIN");
    try {
      const task = await this._createTaskWithinTransaction(input, actor, {
        source: actor.source || "user"
      });
      await this.client.query("COMMIT");
      return task;
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
  }

  async updatePerson(personId, input = {}, actor = {}) {
    await this.ensureReady();
    await this.client.query("BEGIN");
    try {
      const person = await this._updatePersonWithinTransaction(personId, input, actor, {
        source: actor.source || "user"
      });
      await this.client.query("COMMIT");
      return person;
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
  }

  async updateTask(taskId, input = {}, actor = {}) {
    await this.ensureReady();
    await this.client.query("BEGIN");
    try {
      const task = await this._updateTaskWithinTransaction(taskId, input, actor, {
        source: actor.source || "user"
      });
      await this.client.query("COMMIT");
      return task;
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
  }

  async _updatePersonWithinTransaction(personId, input = {}, actor = {}, options = {}) {
    const resolvedActor = buildActor(actor, options.source || "user");
    const opsSchema = quoteIdentifier(this.opsSchema);
    const fields = [];
    const values = [];
    const addField = (column, value, cast = "") => {
      values.push(value);
      fields.push(`${column} = $${values.length}${cast}`);
    };

    if ("full_name" in input || "fullName" in input) {
      addField("full_name", trimText(input.full_name || input.fullName));
    }
    if ("role" in input) addField("role", trimText(input.role));
    if ("phone" in input) addField("phone", trimText(input.phone) || null);
    if ("email" in input) addField("email", trimText(input.email).toLowerCase() || null);
    if ("accent_color" in input || "accentColor" in input) {
      addField("accent_color", trimText(input.accent_color || input.accentColor) || null);
    }
    if ("notes" in input) addField("notes", trimText(input.notes) || null);
    if ("is_active" in input) addField("is_active", input.is_active, "::boolean");
    if ("isActive" in input) addField("is_active", input.isActive, "::boolean");

    if (!fields.length) {
      throw new Error("person_patch_empty");
    }

    values.push(personId);
    const response = await this.client.query(
      `
        UPDATE ${opsSchema}.people
        SET ${fields.join(", ")}
        WHERE id = $${values.length}::uuid
        RETURNING
          id,
          full_name,
          role,
          phone,
          email,
          accent_color,
          notes,
          is_active
      `,
      values
    );
    if (!response.rows.length) {
      throw new Error("person_not_found");
    }

    await this.recordAuditLog(
      {
        actor: resolvedActor,
        eventType: "person.updated",
        entityType: "person",
        entityId: response.rows[0].id,
        details: input
      },
      { skipReadyCheck: true }
    );
    await this.emitChangeEvent(
      {
        type: "person.updated",
        changedDomains: ["people"],
        payload: {
          person_id: response.rows[0].id
        }
      },
      { skipReadyCheck: true }
    );

    return response.rows[0];
  }

  async _replaceTaskAssignmentsWithinTransaction(taskId, assigneeIds = []) {
    const opsSchema = quoteIdentifier(this.opsSchema);
    await this.client.query(
      `
        DELETE FROM ${opsSchema}.task_assignees
        WHERE task_id = $1::uuid
      `,
      [taskId]
    );

    for (const personId of Array.from(new Set((assigneeIds || []).filter(Boolean)))) {
      await this.client.query(
        `
          INSERT INTO ${opsSchema}.task_assignees
            (task_id, person_id)
          VALUES
            ($1::uuid, $2::uuid)
          ON CONFLICT (task_id, person_id) DO NOTHING
        `,
        [taskId, personId]
      );
    }
  }

  async _updateTaskWithinTransaction(taskId, input = {}, actor = {}, options = {}) {
    const resolvedActor = buildActor(actor, options.source || "user");
    const opsSchema = quoteIdentifier(this.opsSchema);
    const fields = [];
    const values = [];
    const addField = (column, value, cast = "") => {
      values.push(value);
      fields.push(`${column} = $${values.length}${cast}`);
    };

    if ("title" in input) addField("title", trimText(input.title));
    if ("notes" in input) addField("notes", trimText(input.notes) || null);
    if ("task_type" in input || "taskType" in input) {
      addField("task_type", trimText(input.task_type || input.taskType));
    }
    if ("status" in input) addField("status", trimText(input.status));
    if ("priority" in input) addField("priority", trimText(input.priority));
    if ("start_at" in input || "startAt" in input) {
      addField("start_at", input.start_at || input.startAt, "::timestamptz");
    }
    if ("end_at" in input || "endAt" in input) {
      addField("end_at", input.end_at || input.endAt, "::timestamptz");
    }
    if ("property_label" in input || "propertyLabel" in input) {
      addField("property_label", trimText(input.property_label || input.propertyLabel));
    }
    if ("reservation_uuid" in input || "reservationUUID" in input) {
      addField("reservation_uuid", input.reservation_uuid || input.reservationUUID || null, "::uuid");
    }

    const wantsAssignmentRewrite =
      "assignee_ids" in input ||
      "assigneeIDs" in input ||
      "assignee_names" in input ||
      "assigneeNames" in input ||
      "assignee_emails" in input ||
      "assigneeEmails" in input;

    if (!fields.length && !wantsAssignmentRewrite) {
      throw new Error("task_patch_empty");
    }

    let task = null;
    if (fields.length) {
      values.push(taskId);
      const response = await this.client.query(
        `
          UPDATE ${opsSchema}.calendar_tasks
          SET ${fields.join(", ")}, updated_at = NOW()
          WHERE id = $${values.length}::uuid
          RETURNING
            id,
            title,
            notes,
            task_type,
            status,
            priority,
            start_at,
            end_at,
            property_label,
            reservation_uuid,
            created_at,
            updated_at
        `,
        values
      );
      if (!response.rows.length) {
        throw new Error("task_not_found");
      }
      task = response.rows[0];
    } else {
      const response = await this.client.query(
        `
          SELECT
            id,
            title,
            notes,
            task_type,
            status,
            priority,
            start_at,
            end_at,
            property_label,
            reservation_uuid,
            created_at,
            updated_at
          FROM ${opsSchema}.calendar_tasks
          WHERE id = $1::uuid
          LIMIT 1
        `,
        [taskId]
      );
      if (!response.rows.length) {
        throw new Error("task_not_found");
      }
      task = response.rows[0];
    }

    let resolvedAssigneeIds = null;
    if (wantsAssignmentRewrite) {
      const availablePeople = options.availablePeople || (await this.listPeopleForAssignment({ skipReadyCheck: true }));
      resolvedAssigneeIds = this.resolveAssigneeIds(input, availablePeople);
      await this._replaceTaskAssignmentsWithinTransaction(taskId, resolvedAssigneeIds);
    }

    await this.recordAuditLog(
      {
        actor: resolvedActor,
        eventType: "task.updated",
        entityType: "calendar_task",
        entityId: task.id,
        details: {
          ...input,
          ...(resolvedAssigneeIds ? { assignee_ids: resolvedAssigneeIds } : {})
        }
      },
      { skipReadyCheck: true }
    );
    await this.emitChangeEvent(
      {
        type: "task.updated",
        changedDomains: resolvedAssigneeIds ? ["tasks", "assignments"] : ["tasks"],
        recommendedWindow: {
          from: task.start_at,
          to: task.end_at
        },
        payload: {
          task_id: task.id
        }
      },
      { skipReadyCheck: true }
    );

    return task;
  }

  async createAgentThread(actor = {}, input = {}) {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const resolvedActor = buildActor(actor);
    const agentId = normalizeAgentId(input.agent_id || input.agentId);
    assertActorCanUseAgent(agentId, resolvedActor);
    const response = await this.client.query(
      `
        INSERT INTO ${opsSchema}.agent_threads (id, created_by, created_by_email, status)
        VALUES ($1::uuid, $2::uuid, $3, 'open')
        RETURNING id, created_at, updated_at
      `,
      [randomUUID(), resolvedActor.userId, resolvedActor.email || null]
    );
    return {
      ...response.rows[0],
      agent_id: agentId
    };
  }

  async postAgentMessage(threadId, input = {}, actor = {}) {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const resolvedActor = buildActor(actor);
    const agentId = normalizeAgentId(input.agent_id || input.agentId);
    assertActorCanUseAgent(agentId, resolvedActor);
    const interactionMode = normalizeInteractionMode(input.interaction_mode || input.interactionMode);
    const attachments = Array.isArray(input.attachments) ? input.attachments : [];
    const transcript = trimText(input.transcript);
    const message = trimText(input.message || transcript);
    if (!message) {
      throw new Error("agent_message_required");
    }

    await this.client.query("BEGIN");
    let userMessageRow;
    let persistedVincentSessionId = "";
    try {
      const threadResponse = await this.client.query(
        `
          SELECT id, created_by
          FROM ${opsSchema}.agent_threads
          WHERE id = $1::uuid
          LIMIT 1
        `,
        [threadId]
      );
      if (!threadResponse.rows.length) {
        throw new Error("agent_thread_not_found");
      }
      assertAgentThreadAccess(threadResponse.rows[0], resolvedActor);
      const latestAssistantMessageResponse = await this.client.query(
        `
          SELECT metadata
          FROM ${opsSchema}.agent_messages
          WHERE thread_id = $1::uuid
            AND role = 'assistant'
          ORDER BY created_at DESC
          LIMIT 1
        `,
        [threadId]
      );
      persistedVincentSessionId = trimText(
        latestAssistantMessageResponse.rows[0]?.metadata?.vincent_session_id || ""
      );
      const insertedUserMessage = await this.client.query(
        `
          INSERT INTO ${opsSchema}.agent_messages
            (id, thread_id, role, content, created_by, metadata)
          VALUES
            ($1::uuid, $2::uuid, 'user', $3, $4::uuid, $5::jsonb)
          RETURNING id, thread_id, role, content, created_at
        `,
        [
          randomUUID(),
          threadId,
          message,
          resolvedActor.userId,
          JSON.stringify({
            user_email: resolvedActor.email || null,
            agent_id: agentId,
            interaction_mode: interactionMode,
            transcript: transcript || null,
            attachments
          })
        ]
      );
      userMessageRow = insertedUserMessage.rows[0];
      await this.client.query(
        `UPDATE ${opsSchema}.agent_threads SET updated_at = NOW() WHERE id = $1::uuid`,
        [threadId]
      );
      await this.client.query("COMMIT");
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }

    const outcome = await this.buildAgentOutcome(message, threadId, resolvedActor, agentId, {
      interactionMode,
      transcript,
      attachments,
      vincentSessionId:
        trimText(input.vincent_session_id || input.vincentSessionId || "") ||
        persistedVincentSessionId
    });

    await this.client.query("BEGIN");
    try {
      const assistantMessageId = randomUUID();
      const assistantMessageResponse = await this.client.query(
        `
          INSERT INTO ${opsSchema}.agent_messages
            (id, thread_id, role, content, metadata)
          VALUES
            ($1::uuid, $2::uuid, 'assistant', $3, $4::jsonb)
          RETURNING id, thread_id, role, content, created_at
        `,
        [
          assistantMessageId,
          threadId,
          outcome.reply,
          JSON.stringify({
            pending_action_ids: outcome.pendingActions.map((action) => action.id),
            agent_id: agentId,
            interaction_mode: interactionMode,
            ...(outcome.metadata || {})
          })
        ]
      );

      const pendingActions = [];
      for (const action of outcome.pendingActions) {
        const response = await this.client.query(
          `
            INSERT INTO ${opsSchema}.agent_actions
              (
                id,
                thread_id,
                message_id,
                action_type,
                target_system,
                status,
                summary,
                command_payload,
                created_by
              )
            VALUES
              ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8::jsonb, $9::uuid)
            RETURNING
              id,
              thread_id,
              action_type,
              target_system,
              status,
              summary,
              command_payload,
              result_payload,
              created_at,
              updated_at
          `,
          [
            action.id,
            threadId,
            assistantMessageId,
            action.action_type,
            action.target_system,
            action.status,
            action.summary,
            JSON.stringify(action.command_payload || {}),
            resolvedActor.userId
          ]
        );
        pendingActions.push(normalizeActionRow(response.rows[0]));
      }

      await this.client.query(
        `UPDATE ${opsSchema}.agent_threads SET updated_at = NOW() WHERE id = $1::uuid`,
        [threadId]
      );

      await this.recordAuditLog(
        {
          actor: resolvedActor,
          eventType: "agent.message",
          entityType: "agent_thread",
          entityId: threadId,
          details: {
            user_message_id: userMessageRow.id,
            assistant_message_id: assistantMessageResponse.rows[0].id,
            pending_actions: pendingActions.map((entry) => entry.id)
          }
        },
        { skipReadyCheck: true }
      );
      await this.client.query("COMMIT");

      return {
        thread_id: threadId,
        reply: outcome.reply,
        assistant_message: normalizeMessageRow(assistantMessageResponse.rows[0], pendingActions),
        pending_actions: pendingActions
      };
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
  }

  async confirmAgentAction(actionId, actor = {}) {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const resolvedActor = buildActor(actor);

    await this.client.query("BEGIN");
    try {
      const actionResponse = await this.client.query(
        `
          SELECT
            id,
            thread_id,
            action_type,
            target_system,
            status,
            summary,
            command_payload,
            result_payload,
            created_by,
            created_at,
            updated_at
          FROM ${opsSchema}.agent_actions
          WHERE id = $1::uuid
          FOR UPDATE
        `,
        [actionId]
      );
      if (!actionResponse.rows.length) {
        throw new Error("agent_action_not_found");
      }
      const action = actionResponse.rows[0];
      assertAgentActionAccess(action, resolvedActor);
      if (action.status !== "pending_approval") {
        throw new Error(`agent_action_invalid_state:${action.status}`);
      }

      let finalAction = null;
      if (action.action_type === "task.create") {
        await this.client.query(
          `
            UPDATE ${opsSchema}.agent_actions
            SET
              status = 'executing',
              confirmed_by = $2::uuid,
              confirmed_by_email = $3,
              confirmed_at = NOW(),
              updated_at = NOW()
            WHERE id = $1::uuid
          `,
          [actionId, resolvedActor.userId, resolvedActor.email || null]
        );

        const createdTask = await this._createTaskWithinTransaction(action.command_payload || {}, resolvedActor, {
          source: "agent"
        });
        const executionResult = {
          status: "succeeded",
          task_id: createdTask.id
        };

        await this.client.query(
          `
            INSERT INTO ${opsSchema}.command_executions
              (
                id,
                action_id,
                actor_user_id,
                actor_email,
                source,
                target_system,
                idempotency_key,
                request_payload,
                result_payload,
                status,
                created_at,
                finished_at
              )
            VALUES
              (
                $1::uuid,
                $2::uuid,
                $3::uuid,
                $4,
                'agent',
                'ops',
                $5,
                $6::jsonb,
                $7::jsonb,
                'succeeded',
                NOW(),
                NOW()
              )
          `,
          [
            randomUUID(),
            actionId,
            resolvedActor.userId,
            resolvedActor.email || null,
            `agent-task-${actionId}`,
            JSON.stringify(action.command_payload || {}),
            JSON.stringify(executionResult)
          ]
        );

        const updatedActionResponse = await this.client.query(
          `
            UPDATE ${opsSchema}.agent_actions
            SET
              status = 'succeeded',
              result_payload = $2::jsonb,
              confirmed_by = $3::uuid,
              confirmed_by_email = $4,
              confirmed_at = NOW(),
              updated_at = NOW()
            WHERE id = $1::uuid
            RETURNING
              id,
              thread_id,
              action_type,
              target_system,
              status,
              summary,
              command_payload,
              result_payload,
              created_at,
              updated_at
          `,
          [
            actionId,
            JSON.stringify(executionResult),
            resolvedActor.userId,
            resolvedActor.email || null
          ]
        );
        finalAction = normalizeActionRow(updatedActionResponse.rows[0]);
      } else if (action.action_type === "ops.plan.apply") {
        await this.client.query(
          `
            UPDATE ${opsSchema}.agent_actions
            SET
              status = 'executing',
              confirmed_by = $2::uuid,
              confirmed_by_email = $3,
              confirmed_at = NOW(),
              updated_at = NOW()
            WHERE id = $1::uuid
          `,
          [actionId, resolvedActor.userId, resolvedActor.email || null]
        );

        const payload = action.command_payload || {};
        const plannedPeople = Array.isArray(payload.people) ? payload.people : [];
        const plannedTasks = Array.isArray(payload.tasks) ? payload.tasks : [];
        const plannedPersonUpdates = Array.isArray(payload.people_updates || payload.peopleUpdates)
          ? payload.people_updates || payload.peopleUpdates
          : [];
        const plannedTaskUpdates = Array.isArray(payload.task_updates || payload.taskUpdates)
          ? payload.task_updates || payload.taskUpdates
          : [];
        if (
          !plannedPeople.length &&
          !plannedTasks.length &&
          !plannedPersonUpdates.length &&
          !plannedTaskUpdates.length
        ) {
          throw new Error("agent_plan_empty");
        }

        const existingPeople = await this.listPeopleForAssignment({ skipReadyCheck: true });
        const matchedPeople = [];
        const createdPeople = [];
        const availablePeople = existingPeople.slice();

        for (const personInput of plannedPeople) {
          const fullNameKey = normalizePersonMatchKey(personInput.full_name || personInput.fullName);
          const emailKey = normalizePersonMatchKey(personInput.email);
          const existingMatch = availablePeople.find((person) => {
            const personNameKey = normalizePersonMatchKey(person.full_name || person.fullName);
            const personEmailKey = normalizePersonMatchKey(person.email);
            return (
              (emailKey && personEmailKey && emailKey === personEmailKey) ||
              (fullNameKey && personNameKey && fullNameKey === personNameKey)
            );
          });
          if (existingMatch) {
            matchedPeople.push(existingMatch);
            continue;
          }
          const createdPerson = await this._createPersonWithinTransaction(personInput, resolvedActor, {
            source: "agent"
          });
          createdPeople.push(createdPerson);
          availablePeople.push(createdPerson);
        }

        const createdTasks = [];
        for (const taskInput of plannedTasks) {
          const resolvedAssigneeIds = this.resolveAssigneeIds(taskInput, availablePeople);
          const createdTask = await this._createTaskWithinTransaction(
            {
              ...taskInput,
              assignee_ids: resolvedAssigneeIds
            },
            resolvedActor,
            {
              source: "agent"
            }
          );
          createdTasks.push(createdTask);
        }

        const updatedPeople = [];
        for (const personUpdate of plannedPersonUpdates) {
          const matchedPerson = await this.findPersonForPlannedUpdate(personUpdate.match, {
            skipReadyCheck: true
          });
          const updatedPerson = await this._updatePersonWithinTransaction(
            matchedPerson.id,
            personUpdate.patch,
            resolvedActor,
            {
              source: "agent"
            }
          );
          updatedPeople.push(updatedPerson);
          const personIndex = availablePeople.findIndex((person) => person.id === updatedPerson.id);
          if (personIndex >= 0) {
            availablePeople[personIndex] = {
              ...availablePeople[personIndex],
              ...updatedPerson
            };
          } else {
            availablePeople.push(updatedPerson);
          }
        }

        const updatedTasks = [];
        for (const taskUpdate of plannedTaskUpdates) {
          const matchedTask = await this.findTaskForPlannedUpdate(taskUpdate.match, {
            skipReadyCheck: true
          });
          const hasAssignmentPatch =
            "assignee_ids" in taskUpdate.patch ||
            "assigneeIDs" in taskUpdate.patch ||
            "assignee_names" in taskUpdate.patch ||
            "assigneeNames" in taskUpdate.patch ||
            "assignee_emails" in taskUpdate.patch ||
            "assigneeEmails" in taskUpdate.patch;
          const resolvedAssigneeIds = hasAssignmentPatch
            ? this.resolveAssigneeIds(taskUpdate.patch, availablePeople)
            : [];
          const updatedTask = await this._updateTaskWithinTransaction(
            matchedTask.id,
            {
              ...taskUpdate.patch,
              ...(hasAssignmentPatch ? { assignee_ids: resolvedAssigneeIds } : {})
            },
            resolvedActor,
            {
              source: "agent",
              availablePeople
            }
          );
          updatedTasks.push(updatedTask);
        }

        const executionResult = {
          status: "succeeded",
          people_created: createdPeople.map((person) => ({
            id: person.id,
            full_name: person.full_name,
            role: person.role
          })),
          people_matched_existing: matchedPeople.map((person) => ({
            id: person.id,
            full_name: person.full_name,
            role: person.role
          })),
          tasks_created: createdTasks.map((task) => ({
            id: task.id,
            title: task.title,
            property_label: task.property_label,
            start_at: task.start_at,
            end_at: task.end_at
          })),
          people_updated: updatedPeople.map((person) => ({
            id: person.id,
            full_name: person.full_name,
            role: person.role
          })),
          tasks_updated: updatedTasks.map((task) => ({
            id: task.id,
            title: task.title,
            property_label: task.property_label,
            start_at: task.start_at,
            end_at: task.end_at
          }))
        };

        await this.client.query(
          `
            INSERT INTO ${opsSchema}.command_executions
              (
                id,
                action_id,
                actor_user_id,
                actor_email,
                source,
                target_system,
                idempotency_key,
                request_payload,
                result_payload,
                status,
                created_at,
                finished_at
              )
            VALUES
              (
                $1::uuid,
                $2::uuid,
                $3::uuid,
                $4,
                'agent',
                'ops',
                $5,
                $6::jsonb,
                $7::jsonb,
                'succeeded',
                NOW(),
                NOW()
              )
          `,
          [
            randomUUID(),
            actionId,
            resolvedActor.userId,
            resolvedActor.email || null,
            `agent-plan-${actionId}`,
            JSON.stringify(payload),
            JSON.stringify(executionResult)
          ]
        );

        const updatedActionResponse = await this.client.query(
          `
            UPDATE ${opsSchema}.agent_actions
            SET
              status = 'succeeded',
              result_payload = $2::jsonb,
              confirmed_by = $3::uuid,
              confirmed_by_email = $4,
              confirmed_at = NOW(),
              updated_at = NOW()
            WHERE id = $1::uuid
            RETURNING
              id,
              thread_id,
              action_type,
              target_system,
              status,
              summary,
              command_payload,
              result_payload,
              created_at,
              updated_at
          `,
          [
            actionId,
            JSON.stringify(executionResult),
            resolvedActor.userId,
            resolvedActor.email || null
          ]
        );
        finalAction = normalizeActionRow(updatedActionResponse.rows[0]);
      } else {
        const queueId = randomUUID();
        const idempotencyKey = `agent-${actionId}`;
        await this.client.query(
          `
            INSERT INTO ${opsSchema}.command_queue
              (
                id,
                action_id,
                command_type,
                target_system,
                status,
                source,
                created_by,
                created_by_email,
                idempotency_key,
                payload
              )
            VALUES
              ($1::uuid, $2::uuid, $3, $4, 'queued', 'agent', $5::uuid, $6, $7, $8::jsonb)
          `,
          [
            queueId,
            actionId,
            action.action_type,
            action.target_system,
            resolvedActor.userId,
            resolvedActor.email || null,
            idempotencyKey,
            JSON.stringify(action.command_payload || {})
          ]
        );
        const updatedActionResponse = await this.client.query(
          `
            UPDATE ${opsSchema}.agent_actions
            SET
              status = 'queued',
              confirmed_by = $2::uuid,
              confirmed_by_email = $3,
              confirmed_at = NOW(),
              result_payload = $4::jsonb,
              updated_at = NOW()
            WHERE id = $1::uuid
            RETURNING
              id,
              thread_id,
              action_type,
              target_system,
              status,
              summary,
              command_payload,
              result_payload,
              created_at,
              updated_at
          `,
          [
            actionId,
            resolvedActor.userId,
            resolvedActor.email || null,
            JSON.stringify({
              queue_id: queueId,
              status: "queued"
            })
          ]
        );
        finalAction = normalizeActionRow(updatedActionResponse.rows[0]);
      }

      await this.recordAuditLog(
        {
          actor: resolvedActor,
          eventType: "agent.action.confirmed",
          entityType: "agent_action",
          entityId: actionId,
          details: {
            action_type: action.action_type,
            target_system: action.target_system
          }
        },
        { skipReadyCheck: true }
      );

      await this.client.query("COMMIT");
      return finalAction;
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
  }

  async createVoiceSession(actor = {}, input = {}) {
    const resolvedActor = buildActor(actor);
    const agentId = normalizeAgentId(input.agent_id || input.agentId);
    assertActorCanUseAgent(agentId, resolvedActor);
    if (!this.elevenLabsService) {
      throw new Error(`elevenlabs_voice_not_configured:${agentId}`);
    }

    return this.elevenLabsService.createConversationToken({
      agentId,
      userId: resolvedActor.userId,
      email: resolvedActor.email
    });
  }

  async listChangeEventsAfter(cursor = "0", options = {}) {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const limit = Math.max(1, Math.min(Number.parseInt(`${options.limit || 25}`, 10) || 25, 100));
    const response = await this.client.query(
      `
        SELECT
          id::text AS cursor,
          event_type AS type,
          changed_domains,
          recommended_window,
          payload,
          created_at
        FROM ${opsSchema}.change_events
        WHERE id > $1::bigint
        ORDER BY id ASC
        LIMIT $2
      `,
      [Number.parseInt(`${cursor || "0"}`, 10) || 0, limit]
    );
    return response.rows.map(normalizeStreamEvent);
  }

  async emitChangeEvent(
    { type, changedDomains = [], recommendedWindow = null, payload = {} } = {},
    options = {}
  ) {
    if (!options.skipReadyCheck) {
      await this.ensureReady();
    }
    const opsSchema = quoteIdentifier(this.opsSchema);
    const response = await this.client.query(
      `
        INSERT INTO ${opsSchema}.change_events
          (event_type, changed_domains, recommended_window, payload)
        VALUES ($1, $2::text[], $3::jsonb, $4::jsonb)
        RETURNING
          id::text AS cursor,
          event_type AS type,
          changed_domains,
          recommended_window,
          payload,
          created_at
      `,
      [
        trimText(type, "change"),
        Array.from(new Set(changedDomains.map((entry) => trimText(entry)).filter(Boolean))),
        JSON.stringify(recommendedWindow || {}),
        JSON.stringify(payload || {})
      ]
    );
    return normalizeStreamEvent(response.rows[0]);
  }

  async recordAuditLog(
    { actor = {}, eventType, entityType, entityId, details = {} } = {},
    options = {}
  ) {
    if (!options.skipReadyCheck) {
      await this.ensureReady();
    }
    const resolvedActor = buildActor(actor);
    const opsSchema = quoteIdentifier(this.opsSchema);
    await this.client.query(
      `
        INSERT INTO ${opsSchema}.audit_log
          (actor_user_id, actor_email, source, event_type, entity_type, entity_id, details)
        VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::jsonb)
      `,
      [
        resolvedActor.userId,
        resolvedActor.email || null,
        resolvedActor.source,
        trimText(eventType),
        trimText(entityType),
        trimText(entityId),
        JSON.stringify(details || {})
      ]
    );
  }

  async processCommandQueue(options = {}) {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const limit = Math.max(1, Math.min(Number.parseInt(`${options.limit || 10}`, 10) || 10, 50));
    const queueResponse = await this.client.query(
      `
        SELECT
          id,
          action_id,
          command_type,
          target_system,
          source,
          created_by,
          created_by_email,
          idempotency_key,
          payload,
          attempts
        FROM ${opsSchema}.command_queue
        WHERE status = 'queued'
          AND scheduled_at <= NOW()
        ORDER BY created_at ASC
        LIMIT $1
      `,
      [limit]
    );

    const results = [];
    for (const queueItem of queueResponse.rows) {
      const attemptNumber = Number(queueItem.attempts || 0) + 1;
      try {
        await this.client.query(
          `
            UPDATE ${opsSchema}.command_queue
            SET status = 'executing', attempts = attempts + 1, locked_at = NOW(), updated_at = NOW()
            WHERE id = $1::uuid
          `,
          [queueItem.id]
        );

        const result =
          queueItem.target_system === "wordpress"
            ? await this.controlPlaneService.sendWordPressCommand(queueItem.payload || {})
            : {
                accepted: false,
                error: `unsupported_target_system:${queueItem.target_system}`
              };

        await this.client.query("BEGIN");
        try {
          await this.client.query(
            `
              UPDATE ${opsSchema}.command_queue
              SET
                status = 'succeeded',
                result_payload = $2::jsonb,
                completed_at = NOW(),
                locked_at = NULL,
                updated_at = NOW()
              WHERE id = $1::uuid
            `,
            [queueItem.id, JSON.stringify(result || {})]
          );
          await this.client.query(
            `
              INSERT INTO ${opsSchema}.command_executions
                (
                  id,
                  queue_id,
                  action_id,
                  actor_user_id,
                  actor_email,
                  source,
                  target_system,
                  idempotency_key,
                  request_payload,
                  result_payload,
                  status,
                  created_at,
                  finished_at
                )
              VALUES
                (
                  $1::uuid,
                  $2::uuid,
                  $3::uuid,
                  $4::uuid,
                  $5,
                  $6,
                  $7,
                  $8,
                  $9::jsonb,
                  $10::jsonb,
                  'succeeded',
                  NOW(),
                  NOW()
                )
            `,
            [
              randomUUID(),
              queueItem.id,
              queueItem.action_id,
              queueItem.created_by,
              queueItem.created_by_email || null,
              queueItem.source,
              queueItem.target_system,
              queueItem.idempotency_key,
              JSON.stringify(queueItem.payload || {}),
              JSON.stringify(result || {})
            ]
          );
          if (queueItem.action_id) {
            await this.client.query(
              `
                UPDATE ${opsSchema}.agent_actions
                SET
                  status = 'succeeded',
                  result_payload = $2::jsonb,
                  updated_at = NOW()
                WHERE id = $1::uuid
              `,
              [queueItem.action_id, JSON.stringify(result || {})]
            );
          }
          await this.recordAuditLog(
            {
              actor: {
                userId: queueItem.created_by,
                email: queueItem.created_by_email,
                source: queueItem.source
              },
              eventType: "command.executed",
              entityType: "command_queue",
              entityId: queueItem.id,
              details: {
                command_type: queueItem.command_type,
                target_system: queueItem.target_system
              }
            },
            { skipReadyCheck: true }
          );
          await this.emitChangeEvent(
            {
              type: "command.executed",
              changedDomains: ["reservations", "tasks"],
              recommendedWindow: {
                from: startOfDay().toISOString(),
                to: addDays(startOfDay(), DEFAULT_STREAM_WINDOW_DAYS).toISOString()
              },
              payload: {
                queue_id: queueItem.id,
                action_id: queueItem.action_id
              }
            },
            { skipReadyCheck: true }
          );
          await this.client.query("COMMIT");
          results.push({ id: queueItem.id, status: "succeeded" });
        } catch (error) {
          await this.client.query("ROLLBACK");
          throw error;
        }
      } catch (error) {
        const nextStatus = attemptNumber >= 3 ? "failed" : "queued";
        const nextSchedule = attemptNumber >= 3 ? "NOW()" : "NOW() + INTERVAL '30 seconds'";
        await this.client.query(
          `
            UPDATE ${opsSchema}.command_queue
            SET
              status = '${nextStatus}',
              last_error = $2,
              locked_at = NULL,
              scheduled_at = ${nextSchedule},
              updated_at = NOW()
            WHERE id = $1::uuid
          `,
          [queueItem.id, error?.message || String(error)]
        );
        if (queueItem.action_id && nextStatus === "failed") {
          await this.client.query(
            `
              UPDATE ${opsSchema}.agent_actions
              SET
                status = 'failed',
                result_payload = $2::jsonb,
                updated_at = NOW()
              WHERE id = $1::uuid
            `,
            [
              queueItem.action_id,
              JSON.stringify({
                error: error?.message || String(error)
              })
            ]
          );
        }
        results.push({
          id: queueItem.id,
          status: nextStatus,
          error: error?.message || String(error)
        });
      }
    }

    return results;
  }

  async runReconciliation() {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const response = await this.client.query(
      `
        UPDATE ${opsSchema}.command_queue
        SET status = 'queued', locked_at = NULL, updated_at = NOW()
        WHERE status = 'executing'
          AND locked_at <= NOW() - INTERVAL '5 minutes'
        RETURNING id
      `
    );
    const requeuedCommands = response.rows.length;
    await this.recordAuditLog(
      {
        actor: { source: "worker" },
        eventType: "reconciliation.tick",
        entityType: "ops",
        entityId: "reconciliation",
        details: {
          requeued_commands: requeuedCommands
        }
      },
      { skipReadyCheck: true }
    );
    return this.emitChangeEvent(
      {
        type: "reconcile.tick",
        changedDomains: ["reservations", "people", "tasks"],
        recommendedWindow: {
          from: startOfDay().toISOString(),
          to: addDays(startOfDay(), DEFAULT_STREAM_WINDOW_DAYS).toISOString()
        },
        payload: {
          requeued_commands: requeuedCommands
        }
      },
      { skipReadyCheck: true }
    );
  }

  async buildAgentOutcome(message, threadId, actor, agentId = "vincent", context = {}) {
    const resolvedAgentId = normalizeAgentId(agentId);
    const interactionMode = normalizeInteractionMode(context.interactionMode);
    const conciseLead =
      interactionMode === "voice_call"
        ? "Keep the response brief and natural for spoken playback. "
        : "";
    const taskDraft = resolvedAgentId === "gael" ? null : buildAgentTaskDraft(message);
    if (taskDraft) {
      const startLabel = new Date(taskDraft.start_at).toLocaleString("en-US", {
        dateStyle: "medium",
        timeStyle: "short"
      });
      return {
        reply: `${conciseLead}I drafted a ${taskDraft.task_type.replace(/_/g, " ")} task for ${taskDraft.property_label} at ${startLabel}. Confirm to execute it.`,
        pendingActions: [
          {
            id: randomUUID(),
            thread_id: threadId,
            action_type: "task.create",
            target_system: "ops",
            status: "pending_approval",
            summary: `Create ${taskDraft.task_type.replace(/_/g, " ")} task for ${taskDraft.property_label}`,
            command_payload: taskDraft,
            created_by: actor.userId || null
          }
        ]
      };
    }

    if (resolvedAgentId === "vincent") {
      if (!this.vincentService?.isConfigured?.()) {
        return {
          reply: `${conciseLead}Vincent is not connected yet. The knowledge service is not configured on this backend.`,
          pendingActions: [],
          metadata: {
            vincent_connected: false
          }
        };
      }

      let vincentSessionId = trimText(context.vincentSessionId);
      if (!vincentSessionId) {
        const createdSession = await this.vincentService.createSession({
          purpose: `staff thread ${threadId} for ${actor.email || actor.userId || "unknown_staff"}`
        });
        vincentSessionId = trimText(createdSession.session_id);
      }

      const vincentResponse = await this.vincentService.sendMessage(vincentSessionId, {
        message,
        maxOutputTokens: interactionMode === "voice_call" ? 300 : 700
      });

      return {
        reply:
          trimText(vincentResponse.reply) ||
          `${conciseLead}Vincent did not return a reply.`,
        pendingActions: [],
        metadata: {
          vincent_connected: true,
          vincent_session_id: vincentSessionId,
          vincent_citations: Array.isArray(vincentResponse.citations)
            ? vincentResponse.citations
            : [],
          vincent_website_paths: Array.isArray(vincentResponse.website_paths_considered)
            ? vincentResponse.website_paths_considered
            : [],
          vincent_recommended_actions: Array.isArray(vincentResponse.recommended_actions)
            ? vincentResponse.recommended_actions
            : [],
          vincent_needs_follow_up: Boolean(vincentResponse.needs_follow_up),
          vincent_tools_used: Array.isArray(vincentResponse.tools_used)
            ? vincentResponse.tools_used
            : [],
          vincent_model: trimText(vincentResponse.model)
        }
      };
    }

    if (resolvedAgentId === "gael") {
      if (!actor?.isElevatedStaff) {
        return {
          reply: `${conciseLead}Gael is reserved for managers right now. Employees can continue using Vincent for estate knowledge and live operations.`,
          pendingActions: []
        };
      }

      if (!this.gaelService?.isConfigured?.()) {
        return {
          reply: `${conciseLead}Gael is not connected yet. Add an Anthropic API key to enable the Claude-backed systems agent.`,
          pendingActions: [],
          metadata: {
            gael_connected: false
          }
        };
      }

      const recentMessages = await this.loadRecentThreadMessages(threadId);
      const anthropicMessages = recentMessages.length
        ? recentMessages
        : [{ role: "user", content: trimText(message) }];
      const lowerMessage = `${message || ""}`.toLowerCase();
      const mentionsPeopleRequest = /(employee|manager|staff|person|people|team)/.test(lowerMessage);
      const deterministicTaskDraft = buildAgentTaskDraft(message);
      if (
        deterministicTaskDraft &&
        looksLikeGaelPlanningRequest(message) &&
        !hasNamedStaffHints(message)
      ) {
        const singleFollowUpQuestion = mentionsPeopleRequest ? inferMissingPersonQuestion(message) : "";
        return {
          reply: mentionsPeopleRequest
            ? buildSingleFollowUpReply(singleFollowUpQuestion, "Prepared the task plan.")
            : "- Prepared the task plan.\n- Confirm to apply.",
          pendingActions: [
            buildGaelPlanAction(
              {
                requestedSummary: message,
                people: [],
                tasks: [deterministicTaskDraft]
              },
              threadId,
              actor
            )
          ],
          metadata: {
            gael_connected: true,
            gael_model: "heuristic",
            gael_heuristic_plan: true,
            gael_bypassed_model: true,
            ...(mentionsPeopleRequest ? { gael_needs_follow_up: true, gael_partial_plan: true } : {})
          }
        };
      }
      let dashboardSummary = "Live dashboard summary is currently unavailable.";
      let peopleDirectorySummary = "Current active staff directory is unavailable.";
      let taskDirectorySummary = "Current scheduled tasks are unavailable.";
      let dashboard = null;
      if (this.client) {
        dashboard = await this.getDashboard({
          from: startOfDay().toISOString(),
          to: addDays(startOfDay(), DEFAULT_STREAM_WINDOW_DAYS).toISOString()
        });
        dashboardSummary = `Live dashboard summary: ${dashboard.reservations.length} reservations in window, ${dashboard.tasks.length} scheduled tasks, ${dashboard.people.filter((person) => person.is_active).length} active staff, ${dashboard.assignments.length} assignments.`;
        const activePeopleSummary = dashboard.people
          .filter((person) => person.is_active)
          .slice(0, 25)
          .map((person) => `${person.full_name} (${person.role})`)
          .join(", ");
        peopleDirectorySummary = activePeopleSummary
          ? `Current active staff directory: ${activePeopleSummary}.`
          : "Current active staff directory has no active people.";
        const scheduledTaskSummary = dashboard.tasks
          .slice(0, 25)
          .map(
            (task) =>
              `${task.id}: ${task.title} @ ${task.property_label} starting ${new Date(task.start_at).toISOString()}`
          )
          .join("; ");
        taskDirectorySummary = scheduledTaskSummary
          ? `Current scheduled tasks in window: ${scheduledTaskSummary}.`
          : "Current scheduled tasks in window: none.";
      }

      const shouldAttemptPlanning = looksLikeGaelPlanningRequest(message) || interactionMode !== "voice_call";
      if (shouldAttemptPlanning) {
        const planningSystem = [
          "You are Gael, a manager-only Claude-powered estate systems agent for Exuma Turquoise Resorts.",
          "First decide whether the manager is asking you to change operations state.",
          "If the request implies creating or updating people, staffing, assignments, shifts, tasks, schedules, plans, or turning an idea into work, return a structured execution plan.",
          "If the request is only advisory, return empty create and update arrays.",
          "Return JSON only. Do not wrap the response in markdown.",
          'Schema: {"reply":string,"needs_follow_up":boolean,"follow_up_questions":string[],"people":[{"full_name":string,"role":"Manager"|"Employee","phone":string|null,"email":string|null,"notes":string|null,"is_active":boolean}],"tasks":[{"title":string,"notes":string|null,"task_type":string,"priority":string,"property_label":string,"start_at":string,"end_at":string,"assignee_names":string[],"assignee_emails":string[]}],"people_updates":[{"match":{"id":string|null,"full_name":string|null,"email":string|null},"patch":{"full_name":string|null,"role":"Manager"|"Employee"|null,"phone":string|null,"email":string|null,"notes":string|null,"accent_color":string|null,"is_active":boolean|null}}],"task_updates":[{"match":{"id":string|null,"title":string|null,"property_label":string|null,"start_at":string|null},"patch":{"title":string|null,"notes":string|null,"task_type":string|null,"status":string|null,"priority":string|null,"property_label":string|null,"start_at":string|null,"end_at":string|null,"assignee_names":string[],"assignee_emails":string[]}}]}',
          "Use ISO-8601 timestamps.",
          "Today is 2026-04-09 in America/New_York.",
          "If the manager says today or tomorrow and no time is given, default to 09:00 local with a one hour duration.",
          "Allowed people roles are Manager and Employee only.",
          "Known property labels include KL Cottage, Lake Cottage, Villa Esencia, and Property TBD.",
          "You can safely prepare database mutations for ops.people, ops.calendar_tasks, and ops.task_assignees.",
          "Use people/tasks for creates. Use people_updates/task_updates for edits to existing records.",
          "Ask at most one blocking follow-up question at a time.",
          "If critical details are missing for an execution request, set needs_follow_up to true and put only the highest-leverage question in follow_up_questions.",
          dashboardSummary,
          peopleDirectorySummary,
          taskDirectorySummary,
          `Current staff user: ${actor.email || actor.userId || "unknown"}`
        ].join(" ");

        const planningResponse = await this.gaelService.sendConversation({
          system: planningSystem,
          messages: anthropicMessages,
          maxOutputTokens: interactionMode === "voice_call" ? 320 : 700,
          temperature: 0.1
        });
        const heuristicTaskDraft = buildAgentTaskDraft(message);
        const parsedPlan = normalizeGaelPlan(extractJsonObject(planningResponse.reply), message);
        if (parsedPlan) {
          const summary = buildGaelPlanSummary(parsedPlan);
          const firstFollowUpQuestion = parsedPlan.followUpQuestions[0] || "";
          if (hasGaelPlanChanges(parsedPlan) && !parsedPlan.needsFollowUp) {
            return {
              reply:
                normalizeGaelReply(trimText(parsedPlan.reply) || `Prepared plan to ${summary}. Confirm to apply.`) ||
                `- Prepared plan to ${summary}.\n- Confirm to apply.`,
              pendingActions: [
                buildGaelPlanAction(
                  {
                    ...parsedPlan,
                    requestedSummary: message
                  },
                  threadId,
                  actor
                )
              ],
              metadata: {
                gael_connected: true,
                gael_model: trimText(planningResponse.model),
                gael_stop_reason: trimText(planningResponse.stop_reason),
                gael_usage: planningResponse.usage || {},
                gael_plan_detected: true
              }
            };
          }

          if (parsedPlan.needsFollowUp) {
            const partialTasks = Array.isArray(parsedPlan.tasks) && parsedPlan.tasks.length
              ? parsedPlan.tasks
              : heuristicTaskDraft
                ? [heuristicTaskDraft]
                : [];
            const partialPeople = Array.isArray(parsedPlan.people) ? parsedPlan.people : [];
            const partialPeopleUpdates = Array.isArray(parsedPlan.people_updates) ? parsedPlan.people_updates : [];
            const partialTaskUpdates = Array.isArray(parsedPlan.task_updates) ? parsedPlan.task_updates : [];
            if (partialPeople.length || partialTasks.length || partialPeopleUpdates.length || partialTaskUpdates.length) {
              return {
                reply:
                  buildSingleFollowUpReply(
                    firstFollowUpQuestion || inferMissingPersonQuestion(message),
                    "Prepared the executable part."
                  ) ||
                  normalizeGaelReply(trimText(parsedPlan.reply)),
                pendingActions: [
                  buildGaelPlanAction(
                    {
                      ...parsedPlan,
                      people: partialPeople,
                      tasks: partialTasks,
                      people_updates: partialPeopleUpdates,
                      task_updates: partialTaskUpdates,
                      requestedSummary: message
                    },
                    threadId,
                    actor
                  )
                ],
                metadata: {
                  gael_connected: true,
                  gael_model: trimText(planningResponse.model),
                  gael_stop_reason: trimText(planningResponse.stop_reason),
                  gael_usage: planningResponse.usage || {},
                  gael_plan_detected: true,
                  gael_needs_follow_up: true,
                  gael_partial_plan: true
                }
              };
            }
            const followUpReply =
              (firstFollowUpQuestion ? buildSingleFollowUpReply(firstFollowUpQuestion) : "") ||
              normalizeGaelReply(trimText(parsedPlan.reply)) ||
              normalizeGaelReply(planningResponse.reply) ||
              "- Need more detail before I can prepare an executable plan.";
            return {
              reply: followUpReply,
              pendingActions: [],
              metadata: {
                gael_connected: true,
                gael_model: trimText(planningResponse.model),
                gael_stop_reason: trimText(planningResponse.stop_reason),
                gael_usage: planningResponse.usage || {},
                gael_plan_detected: true,
                gael_needs_follow_up: true
              }
            };
          }
        }

        const planningFallbackReply = normalizeGaelReply(planningResponse.reply);
        if (heuristicTaskDraft) {
          const singleFollowUpQuestion = mentionsPeopleRequest ? inferMissingPersonQuestion(message) : "";
          return {
            reply: mentionsPeopleRequest
              ? buildSingleFollowUpReply(singleFollowUpQuestion, "Prepared the task plan.")
              : "- Prepared the task plan.\n- Confirm to apply.",
            pendingActions: [
              buildGaelPlanAction(
                {
                  requestedSummary: message,
                  people: [],
                  tasks: [heuristicTaskDraft]
                },
                threadId,
                actor
              )
            ],
            metadata: {
              gael_connected: true,
              gael_model: trimText(planningResponse.model),
              gael_stop_reason: trimText(planningResponse.stop_reason),
              gael_usage: planningResponse.usage || {},
              gael_plan_parse_failed: true,
              gael_heuristic_plan: true,
              ...(mentionsPeopleRequest ? { gael_needs_follow_up: true, gael_partial_plan: true } : {})
            }
          };
        }
        if (mentionsPeopleRequest && looksLikeGaelPlanningRequest(message)) {
          return {
            reply: buildSingleFollowUpReply(inferMissingPersonQuestion(message)),
            pendingActions: [],
            metadata: {
              gael_connected: true,
              gael_model: trimText(planningResponse.model),
              gael_stop_reason: trimText(planningResponse.stop_reason),
              gael_usage: planningResponse.usage || {},
              gael_plan_parse_failed: true,
              gael_needs_follow_up: true
            }
          };
        }
        if (planningFallbackReply) {
          return {
            reply: planningFallbackReply,
            pendingActions: [],
            metadata: {
              gael_connected: true,
              gael_model: trimText(planningResponse.model),
              gael_stop_reason: trimText(planningResponse.stop_reason),
              gael_usage: planningResponse.usage || {},
              gael_plan_parse_failed: true
            }
          };
        }
      }

      const systemContext = [
        "You are Gael, a manager-only Claude-powered estate systems agent for Exuma Turquoise Resorts.",
        "Focus on operations strategy, workflow design, staffing coordination, automation ideas, implementation planning, and crisp decision support.",
        "Be direct, skeptical, and neutral. Do not be upbeat, congratulatory, or motivational.",
        "Keep responses short.",
        "Start directly with the list. No preamble.",
        "Maximum 3 items.",
        "Maximum 14 words per item.",
        "Output plain text only.",
        "Use only numbered lists or hyphen bullet lists.",
        "Do not use markdown emphasis, asterisks, headings, emojis, or decorative formatting.",
        "Do not claim an action was executed unless the system explicitly tells you it was completed.",
        dashboardSummary,
        `Current staff user: ${actor.email || actor.userId || "unknown"}`
      ].join(" ");
      const gaelResponse = await this.gaelService.sendConversation({
        system: systemContext,
        messages: anthropicMessages,
        maxOutputTokens: interactionMode === "voice_call" ? 220 : 350
      });

      return {
        reply:
          normalizeGaelReply(gaelResponse.reply) ||
          "- Gael did not return a reply.",
        pendingActions: [],
        metadata: {
          gael_connected: true,
          gael_model: trimText(gaelResponse.model),
          gael_stop_reason: trimText(gaelResponse.stop_reason),
          gael_usage: gaelResponse.usage || {}
        }
      };
    }

    const dashboard = await this.getDashboard({
      from: startOfDay().toISOString(),
      to: addDays(startOfDay(), DEFAULT_STREAM_WINDOW_DAYS).toISOString()
    });
    const todayStart = startOfDay();
    const tomorrowStart = addDays(todayStart, 1);
    const todayArrivals = dashboard.reservations.filter((reservation) => {
      const checkin = reservation.checkin_at ? new Date(reservation.checkin_at) : null;
      return checkin && checkin >= todayStart && checkin < tomorrowStart;
    }).length;
    const todayDepartures = dashboard.reservations.filter((reservation) => {
      const checkout = reservation.checkout_at ? new Date(reservation.checkout_at) : null;
      return checkout && checkout >= todayStart && checkout < tomorrowStart;
    }).length;
    const urgentTasks = dashboard.tasks.filter((task) => task.priority === "urgent").length;
    const unassignedTasks = dashboard.tasks.filter((task) => {
      return !dashboard.assignments.some((assignment) => assignment.task_id === task.id);
    }).length;
    const activeManagers = dashboard.people.filter((person) => `${person.role || ""}`.trim().toLowerCase() === "manager" && person.is_active).length;
    const activeEmployees = dashboard.people.filter((person) => `${person.role || ""}`.trim().toLowerCase() === "employee" && person.is_active).length;
    const upcomingWindowEnd = addDays(todayStart, 3);
    const upcomingArrivals = dashboard.reservations.filter((reservation) => {
      const checkin = reservation.checkin_at ? new Date(reservation.checkin_at) : null;
      return checkin && checkin >= todayStart && checkin < upcomingWindowEnd;
    }).length;
    const upcomingDepartures = dashboard.reservations.filter((reservation) => {
      const checkout = reservation.checkout_at ? new Date(reservation.checkout_at) : null;
      return checkout && checkout >= todayStart && checkout < upcomingWindowEnd;
    }).length;

    if (resolvedAgentId === "tessa") {
      return {
        reply: `${conciseLead}Tessa focus: there are ${dashboard.tasks.length} scheduled tasks, ${urgentTasks} urgent task(s), and ${unassignedTasks} unassigned task(s). Active staffing currently shows ${activeManagers} manager(s) and ${activeEmployees} employee(s).`,
        pendingActions: []
      };
    }

    if (resolvedAgentId === "mira") {
      return {
        reply: `${conciseLead}Mira focus: today there are ${todayArrivals} arrivals and ${todayDepartures} departures. In the next 72 hours there are ${upcomingArrivals} arrivals and ${upcomingDepartures} departures to prepare for.`,
        pendingActions: []
      };
    }

    if (resolvedAgentId === "customer_service") {
      const guestServiceTasks = dashboard.tasks.filter((task) => task.task_type === "guest_service").length;
      return {
        reply: `${conciseLead}Customer Service focus: today there are ${todayArrivals} arrivals, ${todayDepartures} departures, and ${guestServiceTasks} guest service task(s) already on the board. In the next 72 hours there are ${upcomingArrivals} arrivals and ${upcomingDepartures} departures to support. Ask me to help with guest messaging, arrival notes, or service recovery follow-up.`,
        pendingActions: []
      };
    }

    return {
      reply: `${conciseLead}Today there are ${todayArrivals} arrivals, ${todayDepartures} departures, and ${dashboard.tasks.length} scheduled tasks in the active window. ${urgentTasks > 0 ? `${urgentTasks} task(s) are marked urgent.` : "No urgent tasks are currently flagged."}`,
      pendingActions: []
    };
  }

  async _createTaskWithinTransaction(input = {}, actor = {}, options = {}) {
    const resolvedActor = buildActor(actor, options.source || "user");
    const title = trimText(input.title);
    const propertyLabel = trimText(input.property_label || input.propertyLabel);
    const taskType = trimText(input.task_type || input.taskType, "custom");
    const status = trimText(input.status, "scheduled");
    const priority = trimText(input.priority, "normal");
    const startAt = trimText(input.start_at || input.startAt);
    const endAt = trimText(input.end_at || input.endAt);
    const assigneeIds = Array.isArray(input.assignee_ids || input.assigneeIDs)
      ? Array.from(new Set((input.assignee_ids || input.assigneeIDs).filter(Boolean)))
      : [];

    if (!title || !propertyLabel || !startAt || !endAt) {
      throw new Error("task_validation_failed");
    }

    const opsSchema = quoteIdentifier(this.opsSchema);
    const insertedTask = await this.client.query(
      `
        INSERT INTO ${opsSchema}.calendar_tasks
          (
            id,
            title,
            notes,
            task_type,
            status,
            priority,
            start_at,
            end_at,
            property_label,
            reservation_uuid,
            created_by
          )
        VALUES
          (
            $1::uuid,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7::timestamptz,
            $8::timestamptz,
            $9,
            $10::uuid,
            $11::uuid
          )
        RETURNING
          id,
          title,
          notes,
          task_type,
          status,
          priority,
          start_at,
          end_at,
          property_label,
          reservation_uuid,
          created_at,
          updated_at
      `,
      [
        randomUUID(),
        title,
        trimText(input.notes) || null,
        taskType,
        status,
        priority,
        startAt,
        endAt,
        propertyLabel,
        input.reservation_uuid || input.reservationUUID || null,
        resolvedActor.userId
      ]
    );
    const task = insertedTask.rows[0];

    for (const personId of assigneeIds) {
      await this.client.query(
        `
          INSERT INTO ${opsSchema}.task_assignees
            (task_id, person_id)
          VALUES
            ($1::uuid, $2::uuid)
          ON CONFLICT (task_id, person_id) DO NOTHING
        `,
        [task.id, personId]
      );
    }

    await this.recordAuditLog(
      {
        actor: resolvedActor,
        eventType: "task.created",
        entityType: "calendar_task",
        entityId: task.id,
        details: {
          task_type: task.task_type,
          assignee_ids: assigneeIds
        }
      },
      { skipReadyCheck: true }
    );
    await this.emitChangeEvent(
      {
        type: "task.created",
        changedDomains: ["tasks"],
        recommendedWindow: {
          from: task.start_at,
          to: task.end_at
        },
        payload: {
          task_id: task.id
        }
      },
      { skipReadyCheck: true }
    );

    return task;
  }
}
