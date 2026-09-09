# Durable idempotency

Give each business operation an ID that survives request retries and process
restarts. Declare how to find that ID once; callers still just call the job.

```ts
export const chargePayment = defineJob({
  deps: [Payments],
  async handler(input: { tenantId: string; operationId: string; amount: number }, payments, signal) {
    return payments.charge(input.amount, {
      idempotencyKey: JSON.stringify([input.tenantId, input.operationId]),
      signal,
    });
  },
  metadata: {
    idempotencyKey: input => JSON.stringify([input.tenantId, input.operationId]),
  },
});

await chargePayment({ tenantId: "acme", operationId: "payment-123", amount: 2500 }).result();
```

`Payments` is your application service, and must forward the key to a provider that
actually supports idempotency. Persist `operationId` when creating the business
operation, or accept a stable request ID. Generating a new ID every time the caller
retries defeats idempotency. Include tenant identity and distinguish separate
effects within a job when forming downstream keys.

## What core guarantees

Within a backend's storage namespace, the job's registration name and operation
key identify one submission. Concurrent callers share its stable job ID. Retries
retain that ID, and later submissions replay its retained output or failure.
The original retry policy remains in force; changing metadata does not restart an
exhausted operation. A replay does not run hooks or the handler again.

Reusing the operation key with different encoded input rejects with
`IdempotencyConflictError`, before accepting the conflicting work. The default JSON
codec ignores object property insertion order, including nested objects, but
preserves array order. Custom codecs must encode equivalent inputs identically.

`key` still means active-job coalescing: after completion, another call starts new
work. `idempotencyKey` means one durable operation. The two options cannot be
combined, since coalescing could otherwise discard a distinct operation.

Redis stores the identity, original input, delivery policy, and outcome in the
same BullMQ job record. Its atomic custom-ID insertion avoids a separate receipt
write that could succeed or fail independently of enqueueing. Idempotent records
are exempt from TTL/count cleanup; they consume storage until explicitly removed
outside this API. Removing records removes that protection. Memory offers the same
replay behavior within one backend instance, but loses everything on close/restart.

## Protect effects where they commit

A worker can commit an effect and crash before acknowledging the job. Recovery
then runs the handler again. Queue-side identity cannot make an external effect
and the queue acknowledgement atomic. The handler's dependencies must close that
gap using the same operation identity.

For a database effect, store a receipt with a unique constraint such as
`PRIMARY KEY (tenant_id, operation_id)`. In one transaction, either return the
existing receipt after checking the payload, or apply the business change and
insert its receipt/result. Both must commit or roll back together. A separate
"processed" flag written before or after the effect leaves a crash window.

The executable [SQLite ledger fixture](../packages/core/test/fixtures/ledger.mjs)
demonstrates this transaction. The [crash test](../packages/core/test/durability.test.mjs)
kills a worker after the balance and receipt commit but before queue acknowledgement.
A different worker receives the job again and returns the receipt: two deliveries,
one credit. Another test drops an HTTP response after commit and verifies the same
downstream key prevents a second credit on retry.

Remote APIs need provider-supported idempotency keys and a retention window that
covers your retries. For effects across multiple systems, protect each separately;
one local receipt cannot atomically cover an unrelated external API call. Hooks
with effects need the same care. A genuinely new operation needs a new key; do not
change keys merely to bypass an uncertain failure.

Redis persistence and backup policy determine whether queue records survive data
loss. The Redis crash test proves AOF recovery with `appendfsync always`; it does
not promise lossless recovery for every server configuration. Database/provider
receipts remain necessary even with persistent Redis.

BullMQ documents [custom-ID duplicate suppression](https://docs.bullmq.io/guide/jobs/job-ids)
and the need for [idempotent handlers](https://docs.bullmq.io/patterns/idempotent-jobs).
