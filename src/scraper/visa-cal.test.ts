import { CompanyTypes, type ScraperOptions } from "israeli-bank-scrapers";
import VisaCalScraper from "israeli-bank-scrapers/lib/scrapers/visa-cal.js";
import { fetchPostWithinPage } from "israeli-bank-scrapers/lib/helpers/fetch.js";
import { mock } from "jest-mock-extended";

type ScraperPage = Parameters<NonNullable<ScraperOptions["preparePage"]>>[0];

const apiBase = "https://api.cal-online.co.il";
const framesUrl = `${apiBase}/Frames/api/Frames/GetFrameStatus`;
const pendingUrl = `${apiBase}/Transactions/api/approvals/getClearanceRequests`;
const transactionsUrl = `${apiBase}/Transactions/api/transactionsDetails/getCardTransactionsDetails`;

class TestVisaCalScraper extends VisaCalScraper {
  constructor(page: ScraperPage) {
    super({
      companyId: CompanyTypes.visaCal,
      startDate: new Date("2026-09-01T00:00:00Z"),
      futureMonthsToScrape: 0,
    });
    this.page = page;
  }
}

describe("VisaCal browser-session API requests", () => {
  const page = mock<ScraperPage>();
  let scraper: TestVisaCalScraper;

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-09-28T12:00:00Z"));
    jest.clearAllMocks();
    page.evaluate.mockReset();
    jest
      .spyOn(globalThis, "fetch")
      .mockRejectedValue(
        new Error("VisaCal must not make API requests outside the browser"),
      );

    scraper = new TestVisaCalScraper(page);
    jest
      .spyOn(scraper, "getCards")
      .mockResolvedValue([{ cardUniqueId: "test-card", last4Digits: "1234" }]);
    jest
      .spyOn(scraper, "getAuthorizationHeader")
      .mockResolvedValue("CALAuthScheme test-token");
    jest.spyOn(scraper, "getXSiteId").mockResolvedValue("test-site-id");
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  it("fetches frames, pending charges and transactions in the authenticated page", async () => {
    page.evaluate
      .mockResolvedValueOnce([
        JSON.stringify({
          result: {
            calIssuedCards: {
              frameLimitForCardAmount: 1000,
              cardLevelFrames: [
                {
                  cardUniqueId: "test-card",
                  nextTotalDebit: 50,
                  nextDebitDate: "2026-10-02",
                },
              ],
            },
          },
        }),
        200,
      ])
      .mockResolvedValueOnce([
        JSON.stringify({
          statusCode: 1,
          result: { cardsList: [] },
        }),
        200,
      ])
      .mockResolvedValueOnce([
        JSON.stringify({
          statusCode: 1,
          result: {
            bankAccounts: [
              {
                debitDates: [
                  {
                    transactions: [
                      {
                        trnTypeCode: "5",
                        trnIntId: "test-transaction",
                        trnPurchaseDate: "2026-09-20",
                        debCrdDate: "2026-10-02",
                        amtBeforeConvAndIndex: 50,
                        trnAmt: 50,
                        trnCurrencySymbol: "ILS",
                        debCrdCurrencySymbol: "ILS",
                        merchantName: "Test merchant",
                        transTypeCommentDetails: "",
                        branchCodeDesc: "Test category",
                      },
                    ],
                  },
                ],
                immidiateDebits: { debitDays: [] },
              },
            ],
          },
        }),
        200,
      ]);

    const resultPromise = scraper.fetchData();
    const assertion = expect(resultPromise).resolves.toMatchObject({
      success: true,
      accounts: [
        {
          accountNumber: "1234",
          balance: -50,
          balanceDate: "2026-10-02",
          cardFrame: 1000,
          txns: [
            {
              identifier: "test-transaction",
              description: "Test merchant",
              chargedAmount: -50,
              status: "completed",
            },
          ],
        },
      ],
    });
    await Promise.all([assertion, jest.runAllTimersAsync()]);

    expect(globalThis.fetch).not.toHaveBeenCalled();
    expect(page.evaluate).toHaveBeenCalledTimes(3);
    const headers = {
      Authorization: "CALAuthScheme test-token",
      "X-Site-Id": "test-site-id",
      "Content-Type": "application/json",
    };
    expect(page.evaluate).toHaveBeenNthCalledWith(
      1,
      expect.any(Function),
      framesUrl,
      { cardsForFrameData: [{ cardUniqueId: "test-card" }] },
      headers,
    );
    expect(page.evaluate).toHaveBeenNthCalledWith(
      2,
      expect.any(Function),
      pendingUrl,
      { cardUniqueIDArray: ["test-card"] },
      headers,
    );
    expect(page.evaluate).toHaveBeenNthCalledWith(
      3,
      expect.any(Function),
      transactionsUrl,
      { cardUniqueId: "test-card", month: "9", year: "2026" },
      headers,
    );
  });

  it.each([200, 403, 502])(
    "reports non-JSON HTTP %i responses without leaking response data or tokens",
    async (status) => {
      page.evaluate.mockResolvedValue([
        "<html><body>private-response-data</body></html>",
        status,
      ]);
      const assertion = expect(scraper.fetchData()).rejects.toEqual(
        new Error(
          `fetchPostWithinPage returned invalid JSON, url: ${framesUrl}, status: ${status}`,
        ),
      );
      await Promise.all([assertion, jest.runAllTimersAsync()]);
    },
  );

  it("includes browser cookies and JSON headers in the actual fetch callback", async () => {
    page.evaluate.mockResolvedValue(["{}", 200]);
    const headers = {
      Authorization: "CALAuthScheme test-token",
      "Content-Type": "application/json",
    };
    const data = { cardsForFrameData: [{ cardUniqueId: "test-card" }] };
    const request = fetchPostWithinPage(page, framesUrl, data, headers);
    await jest.runAllTimersAsync();
    await request;

    const [callback, ...args] = page.evaluate.mock.calls[0];
    if (typeof callback !== "function") {
      throw new Error("Expected an executable browser fetch callback");
    }
    jest.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("{}", {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await callback(...args);

    expect(globalThis.fetch).toHaveBeenCalledWith(framesUrl, {
      method: "POST",
      credentials: "include",
      headers,
      body: JSON.stringify(data),
    });
  });
});
