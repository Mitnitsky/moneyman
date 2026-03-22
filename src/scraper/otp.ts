import { AccountConfig } from "../types.js";
import { config } from "../config.js";
import { requestOtpCode } from "../bot/notifier.js";
import { createLogger } from "../utils/logger.js";

const logger = createLogger("otp");

const OTP_SUPPORTED_COMPANIES = ["oneZero", "hapoalim"];

/**
 * Creates an OTP code retriever function for accounts that support OTP via Telegram
 */
function createOtpCodeRetriever(account: AccountConfig): () => Promise<string> {
  return async () => {
    if (!config.options.notifications.telegram?.enableOtp) {
      throw new Error("OTP is not enabled in configuration");
    }

    const phoneNumber = (account as any).phoneNumber || "N/A";
    logger(
      `Requesting OTP code for ${account.companyId} account (phone: ${phoneNumber})`,
    );
    return await requestOtpCode(account.companyId, phoneNumber);
  };
}

/**
 * Checks if an account should have an OTP code retriever attached
 */
export function shouldCreateOtpRetriever(account: AccountConfig): boolean {
  if (!OTP_SUPPORTED_COMPANIES.includes(account.companyId)) return false;
  if (config.options.notifications.telegram?.enableOtp !== true) return false;
  if ("otpLongTermToken" in account) return false;

  // OneZero requires phoneNumber, hapoalim does not (bank sends SMS automatically)
  if (account.companyId === "oneZero") {
    return "phoneNumber" in account && !!account.phoneNumber;
  }

  return true;
}

/**
 * Prepares the account credentials with OTP support if needed
 */
export function prepareAccountCredentials(
  account: AccountConfig,
): Partial<AccountConfig> {
  if (shouldCreateOtpRetriever(account)) {
    logger(`Setting up OTP code retriever for ${account.companyId} account`);

    return {
      otpCodeRetriever: createOtpCodeRetriever(account),
    };
  }

  return {};
}
