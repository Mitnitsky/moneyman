import { fetchPost } from "israeli-bank-scrapers/lib/helpers/fetch.js";

const endpoint =
  "https://api.cal-online.co.il/Frames/api/Frames/GetFrameStatus";

describe("Scraper POST response diagnostics", () => {
  afterEach(() => jest.restoreAllMocks());

  it.each([200, 401])(
    "preserves valid JSON responses with HTTP %i",
    async (status) => {
      const result = { statusCode: status === 200 ? 1 : 0 };
      const fetchMock = jest
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(new Response(JSON.stringify(result), { status }));
      const data = { cardsForFrameData: [{ cardUniqueId: "test-card" }] };
      await expect(
        fetchPost(endpoint, data, { Authorization: "test-token" }),
      ).resolves.toEqual(result);
      expect(fetchMock).toHaveBeenCalledWith(endpoint, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          Authorization: "test-token",
        },
        body: JSON.stringify(data),
      });
    },
  );

  it.each([200, 403, 502])(
    "reports HTTP %i HTML responses without private data",
    async (status) => {
      jest.spyOn(globalThis, "fetch").mockResolvedValue(
        new Response("<html><body>private-response-data</body></html>", {
          status,
        }),
      );
      await expect(
        fetchPost(
          `${endpoint}?token=private-query#private-fragment`,
          { cardUniqueId: "private-card" },
          { Authorization: "private-token" },
        ),
      ).rejects.toEqual(
        new Error(
          `fetchPost returned invalid JSON, url: ${endpoint}, status: ${status}`,
        ),
      );
    },
  );

  it("preserves network failures instead of misreporting them as JSON errors", async () => {
    const error = new TypeError("fetch failed");
    jest.spyOn(globalThis, "fetch").mockRejectedValue(error);
    await expect(fetchPost(endpoint, {})).rejects.toBe(error);
  });
});
