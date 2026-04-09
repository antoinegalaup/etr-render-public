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

export class StaffOperationsService {
  constructor(options = {}) {
    this.controlPlaneService = options.controlPlaneService || null;
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

  async createPerson(input = {}, actor = {}) {
    await this.ensureReady();
    const resolvedActor = buildActor(actor);
    const fullName = trimText(input.full_name || input.fullName);
    const role = trimText(input.role);
    const phone = trimText(input.phone) || null;
    const email = trimText(input.email) || null;
    const notes = trimText(input.notes) || null;
    const accentColor = trimText(
      input.accent_color || input.accentColor,
      role.toLowerCase() === "manager" ? "#0f766e" : "#1f6feb"
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
    await this.client.query("BEGIN");
    try {
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
      await this.client.query("COMMIT");
      return person;
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
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

  async updateTask(taskId, input = {}, actor = {}) {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const fields = [];
    const values = [];
    const addField = (column, value, cast = "") => {
      values.push(value);
      fields.push(`${column} = $${values.length}${cast}`);
    };

    if ("title" in input) addField("title", trimText(input.title));
    if ("notes" in input) addField("notes", trimText(input.notes) || null);
    if ("task_type" in input) addField("task_type", trimText(input.task_type));
    if ("status" in input) addField("status", trimText(input.status));
    if ("priority" in input) addField("priority", trimText(input.priority));
    if ("start_at" in input) addField("start_at", input.start_at, "::timestamptz");
    if ("end_at" in input) addField("end_at", input.end_at, "::timestamptz");
    if ("property_label" in input) addField("property_label", trimText(input.property_label));
    if ("reservation_uuid" in input) addField("reservation_uuid", input.reservation_uuid, "::uuid");

    if (!fields.length) {
      throw new Error("task_patch_empty");
    }

    values.push(taskId);
    await this.client.query("BEGIN");
    try {
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

      await this.recordAuditLog(
        {
          actor,
          eventType: "task.updated",
          entityType: "calendar_task",
          entityId: response.rows[0].id,
          details: input
        },
        { skipReadyCheck: true }
      );
      await this.emitChangeEvent(
        {
          type: "task.updated",
          changedDomains: ["tasks"],
          recommendedWindow: {
            from: response.rows[0].start_at,
            to: response.rows[0].end_at
          },
          payload: {
            task_id: response.rows[0].id
          }
        },
        { skipReadyCheck: true }
      );
      await this.client.query("COMMIT");
      return response.rows[0];
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
  }

  async createAgentThread(actor = {}, input = {}) {
    await this.ensureReady();
    const opsSchema = quoteIdentifier(this.opsSchema);
    const resolvedActor = buildActor(actor);
    const agentId = normalizeAgentId(input.agent_id || input.agentId);
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
    const interactionMode = normalizeInteractionMode(input.interaction_mode || input.interactionMode);
    const attachments = Array.isArray(input.attachments) ? input.attachments : [];
    const transcript = trimText(input.transcript);
    const message = trimText(input.message || transcript);
    if (!message) {
      throw new Error("agent_message_required");
    }

    await this.client.query("BEGIN");
    let userMessageRow;
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
      attachments
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
            interaction_mode: interactionMode
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
    const taskDraft = buildAgentTaskDraft(message);
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
