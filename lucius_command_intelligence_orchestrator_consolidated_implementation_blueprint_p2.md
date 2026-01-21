# LUCIUS Command Intelligence Orchestrator — Consolidated Implementation Blueprint

**Scope (what we are building now)**
- **LUCIUS Control Plane** (single domain): Command Intelligence / Orchestration.
- **Two deployable services**:
  1) **API + Orchestrator + Decision Maker** (FastAPI)
  2) **Reconciliation Service** (background loops: dispatcher/outbox publisher + sweepers + drift repair)
- **Not building Unified Services (Platform)**, but **defining contracts** they must follow.

---

## 1) Functional requirements

### 1.1 Request types (extensible)
System must support the below request families and allow adding more without code changes to orchestration logic (protocol-driven):
- **Atomic**:
  - `OCR` (PDF/Image → Text)
  - `EMBEDDING` (Text → Vector)
  - `SIS` (Prompt + VectorDoc/Text → Text/JSON)
- **Composed protocols**:
  - `OCR→EMBEDDING`
  - `OCR→EMBEDDING→SIS`
  - `EMBEDDING→SIS`

### 1.2 Request envelope (contract)
All incoming requests must follow a stable envelope. Fields are additive over time.

**Envelope (v1):**
- `tenant_id` (required)
- `request_type` (required; maps to a protocol)
- `mode` (optional; defaults to SAFE/DEFAULT from global config)
- `input_ref` (required; blob/document identifiers)
- `output_ref` (required; final output destination)
- `payload` (required; service-specific params, e.g., chunking, prompt set)
- `idempotency_key` (optional but strongly recommended; if absent derive deterministically when possible)
- `schema_version` (required)

**Notes**
- Orchestrator does **not** parse/consume file contents. It routes metadata and references.
- Platform services are responsible for actual I/O and processing.

### 1.3 Validation
A **validator** runs per request:
- Validate envelope structure, required fields, schema_version compatibility
- Validate that `payload` contains required sub-fields for the protocol steps (syntactic/contract validation)
- Validate output/input refs are well-formed and conform to policy (scheme/domain/container allowlists if any)

**If invalid:** return a specific error code and do not create a job.

### 1.4 Job lifecycle and APIs
- Create **Job** for each accepted request.
- Return **202 Accepted** with `jobId` immediately (async model).
- Provide APIs for other services to query state:
  - `GET /v1/jobs/{jobId}`
  - `GET /v1/jobs/{jobId}/steps`
  - `GET /v1/jobs/{jobId}/events` (optional)
  - `POST /v1/jobs/{jobId}:cancel`

### 1.5 Cancellation semantics
- Cancellation marks job **CANCELLING**.
- **Only current step** is allowed to finish; do not start new steps.
- When current step reaches a terminal (SUCCESS/FAILED), transition job to **CANCELLED**.
- Late callbacks after terminal: accepted for logging but **must not** mutate terminal state.

### 1.6 Retries and failure classes
Two retry tracks:
1) **Dispatch/Ack track** (downstream didn’t ACK pickup)
2) **Execution track** (downstream executed and returned FAILED)

Rules:
- Max attempts per step: **3**.
- If a step fails with **non-retryable** classification or exceeds attempts → mark step `FAILED_FINAL` and job `FAILED_FINAL`.
- Use a DLQ concept for final failures (logical DLQ row/flag + optional Service Bus DLQ).

### 1.7 Sequential step execution
- **Only one step may be IN_PROGRESS per job**.
- Protocol steps are executed sequentially.

---

## 2) Non-functional requirements (baseline)
- **Scalable**: HPA for API + workers; KEDA-driven consumption for Service Bus.
- **Backpressure**: return explicit error when system is saturated; enforce per-tenant and global guardrails.
- **Idempotency**: duplicate requests must not duplicate work.
- **Observability**: job/step audit trail, correlation IDs, metrics/logs/traces.
- **Operational safety**: rollouts, backward-compatible schema evolution.

---

## 3) High-level architecture (components)

### 3.1 Access Point — API / Control Plane (FastAPI)
Responsibilities:
- Authenticate/authorize caller
- Validate request (contract)
- Compute shard vector (e.g., CRC32 on tenant_id or job key)
- Create Job + initial Step rows in Ledger
- Decide protocol and next directive
- Write **Outbox** entry (durable intent to publish directive)
- Return `202 + jobId`

### 3.2 Logic Unit — Orchestration Engine (within same service or internal worker)
Responsibilities:
- Interpret protocol (recipes) and enforce transitions
- Maintain “Rented Execution” lifecycle (lease/attempt model)
- Ensure terminal state immutability

### 3.3 Unified Grid — Azure Service Bus
- Fixed set of global topics (e.g., **16 partitions**): `global-bus-p0 … global-bus-p15`
- Routing ensures logical tenant isolation via deterministic partitioning
- Physical resource guarding via KEDA max replicas + AKS quotas

### 3.4 Master Ledger — Azure Table Storage
- Source of truth for:
  - job metadata
  - step states
  - attempt/lease history
  - outbox rows
  - reconciliation markers

### 3.5 Schematics — Azure App Configuration
- Stores:
  - **Protocols (recipes)**
  - Tenant routing overrides
  - Global config defaults (including `mode` default SAFE/DEFAULT)

**Important**: App Config is the source of truth; services maintain local caches for performance.

### 3.6 Blob Storage
- Workspace/cache for multi-step flows: `/<system>/<tenantId>/<service>/<jobId>/...`
- Platform services write intermediate artifacts to workspace.
- On success, results appear in `output_ref`.

---

## 4) State model

### 4.1 Job states (suggested)
- `RECEIVED` (optional)
- `QUEUED`
- `DISPATCHING`
- `IN_PROGRESS`
- `SUCCEEDED` (terminal)
- `FAILED_RETRY` (transient)
- `FAILED_FINAL` (terminal)
- `CANCELLING`
- `CANCELLED` (terminal)

### 4.2 Step states (suggested)
- `PENDING`
- `DISPATCHING`
- `AWAITING_ACK`
- `IN_PROGRESS`
- `SUCCEEDED` (terminal)
- `FAILED_RETRY`
- `FAILED_FINAL` (terminal)
- `CANCELLED` (terminal)

**Invariant:** terminal states are immutable.

---

## 5) Contracts with Platform Services (Unified Services)

### 5.1 Directive message (Orchestrator → Platform)
A directive is published to Service Bus and includes:
- `jobId`, `tenant_id`, `stepId`, `protocol_id`, `step_type`
- `attempt_no`, `lease_id` (or equivalent attempt/epoch)
- `input_ref`, `workspace_ref`, `output_ref`
- `payload` (step-specific params)
- `correlation_id`, `traceparent`
- `callback_urls` (ACK + RESULT endpoints)

### 5.2 ACK callback (Platform → Orchestrator)
- Purpose: signal **pickup** (not authority for completion)
- Must include: `jobId`, `stepId`, `attempt_no`, `lease_id`, `status=ACKED`, timestamps

### 5.3 RESULT callback (Platform → Orchestrator)
- Purpose: completion signal
- Must include: `jobId`, `stepId`, `attempt_no`, `lease_id`, terminal result, and artifact refs
- `status`: `SUCCEEDED` | `FAILED` (+ failure_class: retryable/non-retryable)

### 5.4 Idempotency and precedence rules
- Platform must treat duplicate directives as idempotent using `(jobId, stepId, attempt_no, lease_id)`.
- Orchestrator must reject callbacks that do not match the current active attempt/lease.
- Late/out-of-order callbacks are recorded but must not mutate terminal state.

---

## 6) Decision-making & routing

### 6.1 Protocol selection
- `request_type` maps to `protocol_id`.
- Protocol defines ordered steps and the platform service owner for each.

### 6.2 Mode selection
Mode can be specified in request or overridden by tenant/global config.
- Global config always defines a default `mode` (SAFE/DEFAULT).
- Tenant config can override.

### 6.3 Routing key and partition hash generation (core routing logic)
This is the **single source of truth** for how an attempt maps to a Service Bus partition.

**Inputs**
- `tenant_id` (required)
- `doc_id` (required for BURST spreading; see below)
- `resolved_mode ∈ {DEFAULT, BURST}` (aka SAFE/DEFAULT vs BURST)
- `N = 16` (number of global partitions; fixed constant)

**Normalization (MUST be deterministic)**
- `tenant_norm = lower(trim(tenant_id))`
- `doc_norm = lower(trim(doc_id))` (when used)
- Encode as UTF-8 bytes

**Routing key selection (per docs)**
- **DEFAULT/SAFE (sticky per tenant):**
  - `routing_key_used = tenant_norm`
- **BURST (spread within tenant using doc):**
  - `routing_key_used = tenant_norm + doc_norm`  // concatenation as per finalized decision
  - If `doc_id` is missing in BURST mode: **reject request** (preferred) or fallback to tenant-only (must be explicitly decided).

**Collision note (engineering risk)**
- Pure concatenation can create ambiguous strings (e.g., `ab`+`c` vs `a`+`bc`). If you want to eliminate this class of bug, use a delimiter and pin the rule forever. For now, this blueprint follows the finalized rule exactly.

If `doc_id` is missing in BURST mode: **reject request** (preferred) or fallback to tenant-only (must be explicitly decided).

**Hash function**
- `h = CRC32(utf8(routing_key_used))` producing an unsigned 32-bit integer

**Partition / lane**
- `lane = h % N`  // yields 0..15

**Message/ordering key (recommended)**
- `message_key = routing_key_used`  // use as Service Bus PartitionKey/SessionId if you enable those features

---

### 6.4 Burst/Normal lane isolation (final)
We will use the **existing 16 lanes (partitions)** for both DEFAULT and BURST. There is **no separate topic grid** per mode.

**Key principle**
- DEFAULT vs BURST changes the **routing key** (and therefore which of the 16 lanes a message maps to).
- The **topic set remains the same**: `global-bus-p0 … global-bus-p15`.

**Topic naming (fixed)**
- `global-bus-p0 … global-bus-p15`

**Routing rule**
- Compute `lane` using the routing key rules in §6.3.
- Publish to: `topic = "global-bus-p{lane}"`
- Set message property: `mode=DEFAULT|BURST`

**Consumption model**
- Platform services create **one subscription per partition** (or per service) and may use `mode` only for observability/metrics, not routing.

**Operational guardrails**
- To prevent BURST from starving DEFAULT, enforce limits via:
  - consumer replica caps per service (KEDA/HPA)
  - per-tenant/global inflight gates in the control plane
  - prioritization inside consumers if required (optional)

---


Lane isolation must be deterministic and stable. We use the **same** `base_partition` in both lanes, and only change the lane mapping.

**Lane selection**
- `lane ∈ {DEFAULT, BURST}` computed once at ingress via precedence + authorization.
- Persist `effective_mode = lane` in the Job row; all steps route using this value.

#### Scenario A (preferred): two topic sets (hard isolation)
**Topic naming**
- DEFAULT lane topics: `global-default-p0 … global-default-p15`
- BURST lane topics: `global-burst-p0 … global-burst-p15`

**Routing rule**
- `topic = "global-{lane}-p{base_partition}"`

**Publish properties**
- `lane`, `tenant_id`, `jobId`, `stepId`, `attempt_no`, `lease_id`, `message_key`

#### Scenario B (acceptable): single topic set + lane property (soft isolation)
**Topic naming**
- `global-bus-p0 … global-bus-p15`

**Routing rule**
- `topic = "global-bus-p{base_partition}"`
- Message property: `lane=DEFAULT|BURST`

**Consumption model**
- Platform services create two subscriptions per partition:
  - `sub-default` with filter `lane='DEFAULT'`
  - `sub-burst` with filter `lane='BURST'`
- Independent KEDA scalers and replica caps per subscription.

**Decision note**
- Prefer Scenario A initially for operational clarity (separate quotas, simpler dashboards).

---

Lane isolation must be deterministic and stable. We use the same shard vector for tenant affinity, then map to a *lane-specific topic namespace*.

**Definitions**
- `base_partition = CRC32(tenant_id) % 16`
- `lane ∈ {DEFAULT, BURST}` (from request override or tenant/global config)

**Topic naming (recommended, simple, explicit)**
- DEFAULT lane topics: `global-default-p0 … global-default-p15`
- BURST lane topics: `global-burst-p0 … global-burst-p15`

**Routing rule**
- Publish directive to: `topic = "global-{lane}-p{base_partition}"`

**Why this approach**
- Keeps tenant affinity identical across lanes (same base partition)
- Allows independent KEDA/HPA limits per lane
- Prevents BURST load from starving DEFAULT when quotas are enforced

**Alternative (single topic set + lane in subscription filter)**
- Keep topics `global-bus-p0..p15`
- Add message property `lane=DEFAULT|BURST`
- Platform services use separate subscriptions per lane with independent KEDA scalers

**Decision note**
- Prefer the *two-topic-sets* model initially for operational clarity (separate quotas, clear dashboards).

---


## 7) Ledger data model (Azure Table Storage)

### 7.1 Tables / entities (suggested)
- **Jobs**: partition by `tenant_id + time_bucket`, rowkey = `jobId`
- **Steps**: partition by `jobId`, rowkey = `stepIndex/stepId`
- **Outbox**: partition by `tenant_id + time_bucket`, rowkey = `outboxId`
- **Events** (optional): append-only audit events

### 7.2 Concurrency control
- Use ETags to enforce:
  - Only one step transitions to `IN_PROGRESS` per job
  - Only the correct attempt/lease can move a step forward

### 7.3 Pinning routing in the ledger (mandatory)
Routing **MUST** be persisted in the ledger **at attempt creation** and reused for any recovery/re-dispatch.

**Pinned per attempt (minimum fields)**
- `resolved_mode` (DEFAULT/BURST)
- `lane` (0..15)
- `routing_key_used` (string; either `tenant_norm` or `tenant_norm|doc_norm`)
- `decision_source` (REQUEST | TENANT_CONFIG | GLOBAL_CONFIG)
- `decision_reason` (free text / code describing why the mode was chosen)

**Hard rule**
- **Never recompute routing for an existing attempt during recovery.**
- Recovery must re-dispatch using the pinned values, even if config changes after creation.

---


## 8) Outbox + Dispatcher + Sweepers (Reconciliation service)

### 8.1 Outbox pattern (durability between ledger and bus)
- API/Orchestrator writes:
  - ledger state update (e.g., step → DISPATCHING)
  - **outbox row** describing the directive to publish
  - in a safe, retryable sequence with idempotency

### 8.2 Dispatcher loop
- Polls outbox rows in `PENDING`
- Publishes to Service Bus
- Marks outbox `SENT` (idempotent)

### 8.3 Sweeper loops (drift repair)
Examples:
- `DISPATCHING` too long → recreate outbox or re-dispatch
- `AWAITING_ACK` timeout → retry dispatch attempt
- `IN_PROGRESS` lease expired → move to retry or final fail
- Jobs stuck in non-terminal states beyond SLA → escalate/mark failed

---

## 9) End-to-end flow

### 9.1 Ingress / job creation
1) Client calls `POST /v1/commands`
2) Validator checks envelope + required payload for protocol
3) Orchestrator creates `jobId`, writes Job + Step[0] rows
4) Orchestrator decides active step and writes Outbox directive
5) Returns `202 Accepted { jobId }`

### 9.2 Dispatch
6) Reconciliation dispatcher publishes directive to correct topic/partition
7) Platform service consumes directive
8) Platform sends ACK callback
9) Platform performs work, writes outputs to workspace and/or output_ref, then sends RESULT callback

### 9.3 Progression (multi-step)
10) Orchestrator records RESULT, transitions step terminal
11) If protocol has next step:
    - create/activate next step
    - write outbox directive for next step
12) Repeat until all steps terminal

### 9.4 Completion
- On success of final step: job → `SUCCEEDED`
- On final failure: job → `FAILED_FINAL`
- On cancel: job → `CANCELLED` after current step reaches terminal

---

## 10) Backpressure & limits
- Explicit error code returned when saturated.
- Apply:
  - per-tenant concurrent job limits
  - global concurrent job limits
  - lane limits (SAFE vs BURST)
- Enforce via:
  - ledger-based counters/leases and/or Redis if needed for fast gates
  - KEDA max replicas + AKS resource quotas

---

## 11) Security
- AuthN/AuthZ on ingress APIs.
- Do not accept arbitrary callback callers: callbacks must be authenticated (mTLS or signed token) and bound to `(jobId, stepId, attempt, lease)`.
- Validate request refs are well-formed and within policy.
- All secrets via managed identity / Kubernetes secrets; no client credential stuffing in payload.

---

## 12) Observability
- Correlation IDs propagated from ingress → directive → callbacks.
- Metrics:
  - job throughput, latency per step, retries, DLQ counts
  - dispatch lag, outbox backlog, sweeper actions
- Logs: structured logs per jobId/stepId.
- Traces: distributed tracing across orchestrator and platform services.

---

## 13) Implementation plan (initial)

### 13.1 Tech stack
- **Azure Infra + AKS**
- **Python + FastAPI** (API + orchestrator)
- **Azure Service Bus** (topics/subscriptions)
- **Azure Table Storage** (ledger/outbox)
- **Azure App Configuration** (protocols + routing overrides)
- **Blob Storage** (workspace + client outputs)
- **Redis (optional)** only if required for fast backpressure gates
- Deployment: **Helm** initially

### 13.2 Local/POC mode
- Provide pluggable adapters:
  - mock bus (in-memory or local broker)
  - mock ledger (Azurite/Table emulator where possible)
  - mock platform services (skeleton consumers that ACK/RESULT)
- Swap adapters via config to run locally or in AKS.

### 13.3 Skeleton platform services (parallel team development)
Provide a ready-to-fork **Platform Service Skeleton** so multiple teammates can implement OCR / EMBEDDING / SIS in parallel with consistent behavior.

**Skeleton goals**
- Standardize:
  - directive consumption
  - ACK + RESULT callbacks
  - idempotency and attempt/lease gating
  - workspace writing conventions
  - consistent error classification (retryable vs non-retryable)

**Baseline skeleton behavior (shared across services)**
1) **Consume** directive from Service Bus subscription
2) **Deduplicate**:
   - Use `(jobId, stepId, attempt_no, lease_id)` as the idempotency key
   - If already processed, do not re-run work; optionally re-emit RESULT if required
3) **ACK quickly** (pickup signal):
   - Call Orchestrator ACK endpoint with required fields + timestamps
4) **Execute** the step:
   - OCR: read `input_ref`, write `workspace_ref/ocr/output.txt`
   - EMBEDDING: read prior text from workspace, write `workspace_ref/embedding/vectors.json`
   - SIS: read vectors/text, apply `payload.prompt_set`, write `workspace_ref/sis/result.json`
   - (Initial implementation can use placeholder logic; focus on contract + lifecycle correctness)
5) **Write artifacts** to workspace using a predictable layout:
   - `/<system>/<tenant>/<jobId>/<stepType>/<attempt>/...`
6) **Send RESULT** (completion signal):
   - `SUCCEEDED` with artifact refs
   - or `FAILED` with `failure_class` = `RETRYABLE | NON_RETRYABLE` and reason

**Required service template components**
- `servicebus_consumer.py`: polling/receiver with controlled concurrency
- `directive_model.py`: directive contract + validation
- `idempotency_store.py`: (initial) blob/table/redis-backed record of processed keys
- `ack_client.py` and `result_client.py`: resilient HTTP clients with retries + backoff
- `workspace_io.py`: read/write helpers for blob workspace
- `error_model.py`: standard error codes + mapping to failure_class
- `health.py`: liveness/readiness + dependency checks

**Developer workflow**
- Each teammate clones the skeleton and only replaces `execute()` and step-specific validation.
- Contract tests run against the Orchestrator mock:
  - emits ACK within N seconds
  - sends RESULT exactly-once per attempt (idempotent on duplicates)
  - respects lease/attempt gating

**Nice-to-have (later)**
- Load test harness for consumer concurrency
- Chaos testing hooks (inject timeouts/failures)

---

## 14) Open points (explicitly deferred, but tracked)
- Precise lane isolation model (topics vs subscriptions) and KEDA policies
- Exact schema for each step payload (OCR chunking, prompt set formats)
- Detailed SLA/timeouts per step type
- Choice of lease_id mechanism (GUID + epoch) and timeout defaults
- Decide best initial idempotency store for platform skeleton (blob vs table vs redis)

