import { performance } from "perf_hooks";
import { getAccountTransactions } from "./scrape.js";
import { AccountConfig, AccountScrapeResult, ScraperConfig } from "../types.js";
import { createLogger } from "../utils/logger.js";
import { loggerContextStore } from "../utils/asyncContext.js";
import { createBrowser, createSecureBrowserContext } from "./browser.js";
import { getFailureScreenShotPath } from "../utils/failureScreenshot.js";
import { ScraperOptions } from "israeli-bank-scrapers";
import { parallelLimit } from "async";

const logger = createLogger("scraper");

export const scraperOptions: Partial<ScraperOptions> = {
  navigationRetryCount: 3,
  viewportSize: { width: 1920, height: 1080 },
  optInFeatures: [
    "mizrahi:pendingIfHasGenericDescription",
    "mizrahi:pendingIfNoIdentifier",
    "mizrahi:pendingIfTodayTransaction",
    "isracard-amex:skipAdditionalTransactionInformation",
  ],
};

export async function scrapeAccounts(
  {
    accounts,
    startDate,
    futureMonthsToScrape,
    parallelScrapers,
    additionalTransactionInformation,
    includeRawTransaction,
  }: ScraperConfig,
  scrapeStatusChanged?: (
    status: Array<string>,
    totalTime?: number,
  ) => Promise<void>,
  onError?: (e: Error, caller: string) => void,
) {
  const start = performance.now();

  logger(`scraping %d accounts`, accounts.length);
  logger(`start date %s`, startDate.toISOString());

  let futureMonths: number | undefined = undefined;
  if (!Number.isNaN(futureMonthsToScrape)) {
    logger(`months to scrap: %d`, futureMonthsToScrape);
    futureMonths = futureMonthsToScrape;
  }

  const status: Array<string> = [];

  logger("Creating a browser");
  let browser = await createBrowser();
  logger(`Browser created, starting to scrape ${accounts.length} accounts`);

  const scrapedCompanyIds = new Set<string>();

  const results = await parallelLimit<AccountConfig, AccountScrapeResult[]>(
    accounts.map((account, i) => async () => {
      const { companyId } = account;
      const label = account.alias || companyId;
      const isDuplicate = scrapedCompanyIds.has(companyId);
      scrapedCompanyIds.add(companyId);

      // If we already scraped this companyId, wait and create a fresh browser
      // to avoid IP-based rate limiting on the same domain
      if (isDuplicate) {
        const cooldownMs = 30_000;
        logger(
          `Duplicate companyId: ${companyId}, waiting ${cooldownMs / 1000}s before recreating browser`,
        );
        await new Promise((resolve) => setTimeout(resolve, cooldownMs));
        try {
          await browser.close();
        } catch {
          // ignore close errors
        }
        browser = await createBrowser();
      }

      const maxAttempts = isDuplicate ? 3 : 1;
      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const browserContext = await createSecureBrowserContext(
          browser,
          companyId,
        );
        const result = await loggerContextStore.run(
          { prefix: `[#${i} ${label}]` },
          async () => {
            try {
              return await scrapeAccount(
                account,
                {
                  browserContext,
                  startDate,
                  companyId,
                  futureMonthsToScrape: futureMonths,
                  storeFailureScreenShotPath:
                    getFailureScreenShotPath(companyId),
                  additionalTransactionInformation,
                  includeRawTransaction,
                  ...scraperOptions,
                },
                async (message, append = false) => {
                  status[i] = append ? `${status[i]} ${message}` : message;
                  return scrapeStatusChanged?.(status);
                },
              );
            } finally {
              try {
                await browserContext.close();
              } catch {
                // context may already be closed
              }
            }
          },
        );

        // Retry on failure for duplicate companyIds (IP rate limiting)
        if (
          !result.result.success &&
          attempt < maxAttempts
        ) {
          const retryDelay = attempt * 30_000;
          logger(
            `[${label}] Attempt ${attempt}/${maxAttempts} failed, retrying in ${retryDelay / 1000}s`,
          );
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
          try {
            await browser.close();
          } catch {
            // ignore close errors
          }
          browser = await createBrowser();
          continue;
        }

        return result;
      }

      // Unreachable, but TypeScript needs it
      throw new Error("Unexpected: all retry attempts exhausted without return");
    }),
    Number(parallelScrapers),
  );
  const duration = (performance.now() - start) / 1000;
  logger(`scraping ended, total duration: ${duration.toFixed(1)}s`);
  await scrapeStatusChanged?.(status, duration);

  try {
    logger(`closing browser`);
    await browser?.close();
  } catch (e) {
    onError?.(e, "browser.close");
    logger(`failed to close browser`, e);
  }

  logger(getStats(results));
  return results;
}

function getStats(results: Array<AccountScrapeResult>) {
  let accounts = 0;
  let transactions = 0;

  for (let { result } of results) {
    if (result.success) {
      accounts += result.accounts?.length ?? 0;
      for (let account of result.accounts ?? []) {
        transactions += account.txns?.length;
      }
    }
  }

  return {
    accounts,
    transactions,
  };
}

async function scrapeAccount(
  account: AccountConfig,
  scraperOptions: ScraperOptions,
  setStatusMessage: (message: string, append?: boolean) => Promise<void>,
) {
  logger(`scraping started`);

  const scraperStart = performance.now();
  const result = await getAccountTransactions(
    account,
    scraperOptions,
    (cid, step) => setStatusMessage(`[${cid}] ${step}`),
  );

  const duration = (performance.now() - scraperStart) / 1000;
  logger(`scraping ended, took ${duration.toFixed(1)}s`);
  await setStatusMessage(`, took ${duration.toFixed(1)}s`, true);

  return {
    companyId: account.companyId,
    alias: account.alias,
    result,
  };
}
