import { describe, expect, it, vi } from "vitest";
import { ApiError, multipartOf, request } from "./api";

function respond(status: number, body: unknown) {
  return vi.fn(async () => new Response(body === null ? null : JSON.stringify(body), { status }));
}

describe("request", () => {
  it("sends JSON to /api and parses the response", async () => {
    const fetcher = respond(200, { ok: true });
    expect(await request("POST", "/tags", { tag: "food" }, fetcher)).toEqual({ ok: true });
    expect(fetcher).toHaveBeenCalledWith("/api/tags", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: '{"tag":"food"}',
    });
  });

  it("returns null for 204", async () => {
    expect(await request("DELETE", "/rules/1", undefined, respond(204, null))).toBeNull();
  });

  it("raises errors with per-item batch errors", async () => {
    const fetcher = respond(422, { detail: "2 invalid items", errors: [{ index: 1, id: "b", message: "Unknown tags" }] });
    const error = await request("POST", "/annotations/batch", {}, fetcher).catch((caught) => caught);
    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 422, detail: "2 invalid items", errors: [{ index: 1, id: "b" }] });
  });

  it("describes FastAPI validation errors", async () => {
    const fetcher = respond(422, { detail: [{ loc: ["body", "items", 0, "id"], msg: "Field required" }] });
    const error = (await request("POST", "/annotations/batch", {}, fetcher).catch((caught) => caught)) as ApiError;
    expect(error.detail).toBe("items.0.id: Field required");
  });

  it("sends uploads as multipart forms", async () => {
    const fetcher = respond(200, { ok: true });
    const file = new File(["a,b"], "card-2025.csv");
    const form = multipartOf("files", [file], { dry_run: false, replace: true });
    await request("POST", "/raw/alice", form, fetcher);
    // (the browser sets the form's content type, with its boundary)
    expect(fetcher).toHaveBeenCalledWith("/api/raw/alice", { method: "POST", headers: {}, body: form });
    expect((form.get("files") as File).name).toBe("card-2025.csv");
    expect([form.get("dry_run"), form.get("replace")]).toEqual(["false", "true"]);
    // Fields left undefined aren't sent
    const unnamed = multipartOf("file", [file], { dry_run: true, backup_name: undefined });
    expect(unnamed.has("backup_name")).toBe(false);
  });

  it("tells errors that aren't JSON, e.g. a proxy's", async () => {
    const fetcher = vi.fn(async () => new Response("Request Entity Too Large", { status: 413 }));
    const error = (await request("POST", "/import", {}, fetcher).catch((caught) => caught)) as ApiError;
    expect(error).toMatchObject({ status: 413, detail: "Request Entity Too Large" });
  });
});
