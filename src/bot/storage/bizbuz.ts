import { createLogger } from "../../utils/logger.js";
import type { TransactionRow, TransactionStorage } from "../../types.js";
import { TransactionStatuses } from "israeli-bank-scrapers/lib/transactions.js";
import { createSaveStats } from "../saveStats.js";
import type { MoneymanConfig } from "../../config.js";
import assert from "node:assert";

const logger = createLogger("BizBuzStorage");

export class BizBuzStorage implements TransactionStorage {
  constructor(private config: MoneymanConfig) {}

  canSave() {
    return Boolean(this.config.storage.bizbuz?.url);
  }

  async saveTransactions(
    txns: Array<TransactionRow>,
    onProgress: (status: string) => Promise<void>,
  ) {
    logger("saveTransactions");

    const bizbuzConfig = this.config.storage.bizbuz;
    assert(bizbuzConfig, "BizBuz configuration not found");

    const nonPendingTxns = txns.filter(
      (txn) => txn.status !== TransactionStatuses.Pending,
    );

    logger(
      `Posting ${nonPendingTxns.length} transactions to ${bizbuzConfig.url}`,
    );

    const payload = {
      familyId: bizbuzConfig.familyId,
      transactions: nonPendingTxns.map((tx) => ({
        uniqueId: tx.uniqueId,
        date: tx.date,
        processedDate: tx.processedDate,
        originalAmount: tx.originalAmount,
        chargedAmount: tx.chargedAmount,
        description: tx.description,
        type: tx.type || "normal",
        memo: tx.memo || undefined,
        category: tx.category || undefined,
        account: tx.account,
        companyId: tx.companyId,
        originalCurrency: tx.originalCurrency,
        chargedCurrency: tx.chargedCurrency || undefined,
        hash: tx.hash,
        identifier: tx.identifier,
        status: tx.status,
        installments: tx.installments || undefined,
      })),
    };

    const [response] = await Promise.all([
      fetch(bizbuzConfig.url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${bizbuzConfig.token}`,
        },
        body: JSON.stringify(payload),
      }),
      onProgress("Sending"),
    ]);

    if (!response.ok) {
      const body = await response.text().catch(() => "");
      logger(`Failed to post transactions: ${response.status} ${body}`);
      throw new Error(
        `Failed to post to BizBuz: ${response.status} ${response.statusText}`,
      );
    }

    const res = (await response.json()) as Record<string, number>;
    logger(`Response: ${JSON.stringify(res)}`);

    const stats = createSaveStats("BizBuzStorage", "bizbuz", txns);
    stats.added = res.processed ?? nonPendingTxns.length;

    return stats;
  }
}
