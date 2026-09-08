import { defineJob } from "core";
import { Auditor, RequestScope } from "../services/request-scope.js";

/** Both dependencies resolve inside the same execution scope, so their ids match. */
export const audit = defineJob({
  name: "audit.stamp",
  deps: [RequestScope, Auditor],

  async handler(input: { action: string }, scope, auditor) {
    return { scopeId: scope.id, stamp: auditor.stamp(input.action) };
  },
});
