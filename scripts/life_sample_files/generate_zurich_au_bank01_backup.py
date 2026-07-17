#!/usr/bin/env python3
"""
Zurich AU reconciliation sample-data generator (Bank01 + Bank02).

Outputs seven reconcilable files spanning both bank scopes:
  Bank01: merchants BU_AU_Life and BU_AU_Ezicover  -> account 012366-836304645
  Bank02: merchant  BU_AU_OnePath                  -> account 012366-836303306

Bank membership is configured per-merchant in MERCHANT_META; per-bank settings
(account number, alias, ...) live in BANK_META. Adding a new bank means a new
BANK_META entry plus one or more merchants pointing at it -- the writers all
route bank-specific values through these tables.

  1. life3_payments.EXT                   (Life3 ingestion - mixed payment/refund/payout)
  2. gdpp_payments.csv                    (GDPP payments - positive payment rows)
  3. gdpp_refunds.csv                     (GDPP refunds  - positive refund rows)
  4. gdpp_payouts.csv                     (GDPP payouts  - positive payout rows)
  5. adyen_settlement_detail_report.csv   (Adyen Settlement: Settled + Refunded; payouts excluded)
  6. balanceplatform_statement_report.csv (Adyen Balance: balanceAdjustment + bankTransfer + cardTransfer)
  7. bankfile.xlsx                        (Bank: TRANSFER lines for bankTransfer; cardTransfer not on bank)

Transaction-type model (mix configurable via TXN_TYPE_MIX):
  payment  -> Life3 (+ amount), GDPP payments, Adyen Settled  -> balanceAdjustment(+)
  refund   -> Life3 (- amount), GDPP refunds,  Adyen Refunded -> balanceAdjustment(-)
  payout   -> Life3 (- amount), GDPP payouts,  (no Adyen)     -> cardTransfer(-)

Cross-file invariants (engine handles id normalisation + amount sign):
  Life3.GDPP_TRANS_DESC == GDPP_payments.payment_id        (for payment rows)
  Life3.GDPP_TRANS_DESC == GDPP_refunds.refund_id          (for refund rows)
  Life3.GDPP_TRANS_DESC == GDPP_payouts.payout_id          (for payout rows)
  GDPP_payments.connector_transaction_id == Adyen Settlement.Psp Reference (Settled)
  GDPP_refunds.connector_refund_id        == Adyen Settlement.Modification Reference (Refunded)
  GDPP_payouts.connector_payout_id        == Balance Platform Transfer Id (cardTransfer)
    A fraction of payouts (PAYOUT_FAIL_RATE) emit TWO cardTransfer rows sharing
    the same Transfer Id but with different Transaction Ids: one Status=booked
    (Amount = -payout) followed seconds later by one Status=fail (Amount =
    +payout, the reversal). The GDPP/Life3 side still has a single entry per
    payout; the recon engine treats the booked+fail pair as a failed payout.
  sum(Settled.Gross Credit) - sum(Refunded.Gross Debit)
    per (Merchant, Batch, hour) == balanceAdjustment.Amount  (booking_hour = creation_hour + 1)
  -sum(balanceAdjustment Amounts since prev bankTransfer) == bankTransfer.Amount
    Payments and refunds both settle with the bank (refunds are already
    netted into balanceAdjustment). Payouts (cardTransfer) live in Balance
    Platform but do NOT settle with the bank; the recon engine must skip
    cardTransfer rows when aggregating Balance Platform -> bankTransfer.
    bankTransfer.Amount is usually negative but can be positive when refunds
    dominate a drain window.
  Bank.Bank Reference == bankTransfer.Reference (SWPE token)
  Bank.Credits == -1 * bankTransfer.Amount  (or written to Debits if positive)
  The bank file ALSO contains N_RANDOM_BANK_DEBITS standalone Debit rows
  with no matching bankTransfer (simulating unrelated bank activity such as
  fees, BPAY, withdrawals); these exercise the recon engine's
  skip-unmatched-row logic.

Run:
    python3 scripts/generate_zurich_au_bank01_data.py

EDIT THE 'CONFIG' BLOCK BELOW to change volumes, distributions, txn-type mix,
fee model, or to inject mismatches for testing the recon's failure paths.
"""
from __future__ import annotations

import csv
import json
import os
import random
import secrets

import openpyxl
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP, getcontext
from typing import Optional

getcontext().prec = 28


# ============================================================================
# ============================ CONFIG (edit me) ==============================
# ============================================================================

# Set to an int (e.g. 42) for reproducible runs; leave as None to randomize
# every run from system entropy. Ids derived from rng (sequence numbers,
# tokens, dates within hour, batch picks) all reshuffle when SEED is None.
SEED: Optional[int] = None

# Per-run nonce appended as a trailing segment to recon join-key IDs
# (payment_id, refund_id, payout_id, connector_*_id, transfer_id, etc.)
# so IDs from this run never collide with IDs from any earlier run.
# 8 hex chars => 4B-space; birthday collisions only matter past ~50k runs.
RUN_NONCE: str = secrets.token_hex(4).upper()

# Date range. RUN_DATE_END is the latest day in scope; activity is spread
# over N_RUN_DAYS calendar days ending on RUN_DATE_END (inclusive).
# Hop 3 (Adyen Settlement -> Balance Platform) truncates both sides to
# start_of_hour, so within a single (BalanceAccount, calendar day) only one
# balanceAdjustment per hour is allowed -- target adjustments therefore
# spread across multiple days.
RUN_DATE_END: date = date(2026, 6, 8)
N_RUN_DAYS:   int  = 50

OUTPUT_DIR: str = "files/generated/zurich-au-bank01_bank02"

# ---- Volume ---------------------------------------------------------------
# Total Life3 rows you want, across ALL banks. With the default weights below
# (bank01 = 0.5, bank02 = 0.5), 600 -> ~300 rows per bank.
N_LIFE3_TOTAL: int = 1000

# How that total is split across in-scope merchants. Weights must sum to 1.0.
# Bank-level totals fall out of the sum of weights per bank (see MERCHANT_META
# below for each merchant's bank tag). Defaults give an even bank01/bank02
# split:  bank01 = 0.40 + 0.10 = 0.50,  bank02 = 0.50.
MERCHANT_WEIGHTS = {
    "BU_AU_Life":     0.60,  # bank01
    "BU_AU_Ezicover": 0.40,  # bank01
    "BU_AU_OnePath":  0.00,  # bank02
}

# How each merchant's row count is split across transaction types.
# Must sum to 1.0. Refunds and payouts both produce *negative* Life3 rows.
# Refunds aggregate into balanceAdjustment as debit subtractions; payouts
# produce a separate cardTransfer row on Balance Platform.
TXN_TYPE_MIX = {
    "payment": 0.70,
    "refund":  0.20,
    "payout":  0.10,
}

# Refund timing: a refund is created N days after the original payment.
# Range: [REFUND_LAG_DAYS_MIN, REFUND_LAG_DAYS_MAX]. Refund creation_dt is
# clamped to RUN_DATE_END if it would otherwise spill past the run window.
REFUND_LAG_DAYS_MIN: int = 0
REFUND_LAG_DAYS_MAX: int = 14

# Payout amount distribution. Independent from payments since payouts often
# have a different size profile (e.g. larger lump sums). Defaults mirror the
# payment distribution; tweak if you want a different shape.
PAYOUT_AMOUNT_MU:    float   = 5.5
PAYOUT_AMOUNT_SIGMA: float   = 0.7
PAYOUT_AMOUNT_MIN:   Decimal = Decimal("20.00")
PAYOUT_AMOUNT_MAX:   Decimal = Decimal("5000.00")

# Fraction of payouts that fail at the Adyen card-transfer step. Each failed
# payout produces TWO cardTransfer rows in the Balance Platform file:
#   1. a 'booked' row with negative amount (the initial transfer), followed
#      seconds later by
#   2. a 'fail' row with positive amount (the reversal).
# Both rows share the same Transfer Id (matching the single gdpp_payouts
# entry's connector_payout_id) but have different Transaction Ids. The Life3
# and GDPP payouts files stay unchanged (one entry per payout). The recon
# engine treats the booked+fail pair against a single GDPP payout entry as a
# failed payout per the recon spec.
PAYOUT_FAIL_RATE: float = 0.10

# Balance Platform balanceAdjustment row count is automatically tuned to land
# near this fraction of N_LIFE3_TOTAL. With 10k Life3 and ratio 0.10, you'll
# get about 1k rows in the Balance Platform file (plus a handful of
# bankTransfer rows). Override N_BATCHES_PER_MERCHANT_OVERRIDE below if you
# want to set batch counts manually.
BALANCE_TO_LIFE3_RATIO: float = 0.10

# Active hour window per day (inclusive start, exclusive end). Source
# settlement creation_hour falls in [START, END). Booking hour for the
# corresponding balanceAdjustment is creation_hour + 1, so keep END <= 23
# to keep booking_dt inside the same calendar day.
ACTIVE_HOUR_START: int = 0
ACTIVE_HOUR_END:   int = 23

# How much merchants' active-hour windows overlap (0.0 .. 1.0). Grouping key is
# (creation_hour, merchantAccount, settlementBatch), so merchants sharing an
# hour yield multiple balanceAdjustment rows that hour.
#   0.0 -> disjoint windows: one balance row per (date, hour) (legacy).
#   1.0 -> all merchants share the same window: many rows per hour.
# Balance-row count per day is unchanged, so BALANCE_TO_LIFE3_RATIO still holds.
MERCHANT_HOUR_OVERLAP: float = 0.5

# Set to a dict like {"BU_AU_Life": 30, "BU_AU_Ezicover": 12} to override the
# auto-computed batch counts. Set to None to auto-compute.
N_BATCHES_PER_MERCHANT_OVERRIDE: Optional[dict] = None

# ---- Bank-transfer schedule ----------------------------------------------
# Auto-derived from DRAIN_GROUP_SIZE: each BA gets one bankTransfer per N
# balanceAdjustments, fired at the booking_dt of the last adjustment in the
# group. With DRAIN_GROUP_SIZE = 10 and ratio 0.10, total transfers ~= 1% of
# original Life3 row count.
DRAIN_GROUP_SIZE: int = 10

# Optional override: dict[BalanceAccount] -> list of "HH:MM" strings. Set to
# None to auto-derive drain times from DRAIN_GROUP_SIZE.
BANK_TRANSFER_TIMES_OVERRIDE: Optional[dict] = None

# ---- Amount distribution (lognormal, AUD) --------------------------------
AMOUNT_MU:    float   = 5.5            # exp(5.5) ~= 244
AMOUNT_SIGMA: float   = 0.7
AMOUNT_MIN:   Decimal = Decimal("20.00")
AMOUNT_MAX:   Decimal = Decimal("5000.00")

# ---- Fee model (AUD per row) ---------------------------------------------
FEE_MARKUP      = Decimal("0.52")
FEE_SCHEME      = Decimal("1.88")
FEE_INTERCHANGE = Decimal("2.00")

# ---- Card brand mix (weights must sum to 1.0) ----------------------------
CARD_BRANDS = [
    # (display, adyen_pm, adyen_variant, pan_prefix6, weight)
    ("Visa",       "visa", "visapremiumcredit", "411111", 0.60),
    ("Mastercard", "mc",   "mc",                "522222", 0.40),
]

# ---- Per-bank metadata ---------------------------------------------------
# Bank-level settings shared by every merchant in the bank. Add a new bank by
# adding an entry here and pointing one or more MERCHANT_META rows at it via
# the "bank" field.
BANK_META = {
    "bank01": {
        "account_number":   "012366-836304645",
        "account_alias":    "ZURICH AU POLICY",
        "statement_number": "084",
        "opening_balance":  Decimal("2500000.00"),
        "bank_short":       "ZALBANK01",
    },
    "bank02": {
        "account_number":   "012366-836303306",
        "account_alias":    "ZURICH AU POLICY",
        "statement_number": "084",
        "opening_balance":  Decimal("2500000.00"),
        "bank_short":       "ZALBANK02",
    },
}

# ---- Per-merchant fixed metadata -----------------------------------------
# Each merchant tags its bank (-> BANK_META). profile_ids is a list: the
# generator picks one at random per transaction so multiple profiles end up
# represented in Life3 / GDPP for the same merchant. balance_account is still
# 1:1 with the merchant.
MERCHANT_META = {
    "BU_AU_Life": {
        "bank":                "bank01",
        "profile_ids":         [
            "pro_rgUvPqW30Y1vAziqpDCQ",
            "pro_11Rf8SqofUHAA2PZD5TC",
        ],
        "balance_account":     "BA32DBZ22322995MXFFQ6BLKB",
        "account_holder":      "AH32DHQ223229T5NFHG7HB8BP",
        "first_letter":        "L",
        "k_code":              "K336",
        "life3_batch":         "B44",
        "adyen_batch_base":    174,
        "merchant_ref_prefix": "GC",
        "card_token_prefix":   "pm_LIFE3MIT",
    },
    "BU_AU_Ezicover": {
        "bank":                "bank01",
        "profile_ids":         ["pro_RqNOYGa56KofQngh2u46"],
        "balance_account":     "BA32DHM22322995MXFFQ6BQ24",
        "account_holder":      "AH32EZI223229T5NFHG7HB8BP",
        "first_letter":        "E",
        "k_code":              "K336",
        "life3_batch":         "B45",
        "adyen_batch_base":    200,
        "merchant_ref_prefix": "GE",
        "card_token_prefix":   "pm_EZICMIT",
    },
    "BU_AU_OnePath": {
        "bank":                "bank02",
        "profile_ids":         [
            "pro_TreNEjXUsQHGmIhKumoF",
            "pro_l1mXyfyxAuRy891DpDCb",
        ],
        "balance_account":     "BA32DF922322995MXFFQ6D7X2",
        "account_holder":      "AH32ONP223229T5NFHG7HB8BP",
        "first_letter":        "O",
        "k_code":              "K336",
        "life3_batch":         "B46",
        "adyen_batch_base":    230,
        "merchant_ref_prefix": "GO",
        "card_token_prefix":   "pm_OPMIT",
    },
}

# Used when MISMATCH["skip_profile_rate"] > 0; the row will appear in Life3
# with this profile id and will be skipped by the Life3 ingestion's filter.
OFF_SCOPE_PROFILE_ID: str = "pro_OUTOFSCOPE000000000"

# ---- Random unmatched bank debits ----------------------------------------
# Standalone Debit rows that appear in the bank statement but do NOT
# correspond to any balanceAdjustment / bankTransfer. They simulate
# unrelated bank activity (BPAY, direct debits, manual transfers, fees,
# withdrawals) and exercise the recon engine's skip-unmatched-row logic.
#
# Default is 0 because in this dataset the only legitimate source of Debits
# on the bank statement is a bankTransfer whose net (payments minus refunds)
# is negative -- those rows already populate the Debits column via the sign
# of bank_amount in write_bank_statement. Raise this if you specifically
# want to test the recon engine's skip-unmatched-row path.
N_RANDOM_BANK_DEBITS:   int     = 0
RANDOM_BANK_DEBIT_MIN:  Decimal = Decimal("50.00")
RANDOM_BANK_DEBIT_MAX:  Decimal = Decimal("3000.00")

# ---- Forced negative drain windows ---------------------------------------
# With the default txn mix, payments outweigh refunds in every drain window
# so every bankTransfer settles as a Credit. To exercise the negative-net
# path (refunds dominate => bankTransfer.Amount > 0 => Debit on the bank
# statement), we synthesize an oversized "chargeback" refund in N drain
# windows per balance account. The synthetic refund flows through every
# downstream file like a normal refund, so all cross-file invariants hold;
# the only special thing about it is the amount is large enough to flip its
# drain window net-negative.
#
# CHARGEBACK_MULTIPLIER_MIN/MAX controls how oversized the chargeback is
# relative to the drain's current positive net. A value of M means
# synth_amount = M * drain_net, so post-injection drain_net = (1 - M) *
# drain_net. Use M > 1 to flip the sign; the further above 1, the deeper
# negative the resulting drain.
N_FORCED_NEGATIVE_DRAINS_PER_BA: int = 1
CHARGEBACK_MULTIPLIER_MIN:       float = 1.5
CHARGEBACK_MULTIPLIER_MAX:       float = 3.0

# ---- Mismatch injection (set rates > 0 to force recon failures) ----------
MISMATCH = {
    # Life3 row carries an off-scope profile id (skipped at Life3 ingestion).
    "skip_profile_rate":     0.00,
    # GDPP amount differs from Life3 by 1 cent (breaks Hop 1 and Hop 2).
    "amount_mismatch_rate":  0.00,
    # Life3 row has no GDPP counterpart (breaks Hop 1).
    "missing_in_gdpp_rate":  0.00,
    # GDPP row has no Adyen counterpart (breaks Hop 2).
    "missing_in_adyen_rate": 0.00,
    # GDPP status = "failed" instead of "charged" (breaks Hop 1; Hop 2 not
    # triggered; row excluded from Adyen and downstream).
    "status_failed_rate":    0.00,
    # Adjusts a balanceAdjustment.Amount by +/- 0.01 (breaks Hop 3 only;
    # bankTransfer + bank stay consistent with the *true* sum).
    "aggregation_off_rate":  0.00,
}

# ---- Constants you rarely need to change ---------------------------------
COMPANY_ACCOUNT_ADYEN  = "Zurich_Insurance_Group"
BALANCE_PLATFORM_ADYEN = "Zurich_Insurance_Group_AU"
DEFAULT_CURRENCY       = "AUD"

# ============================================================================
# ============================== END CONFIG ==================================
# ============================================================================


# ---------------------------- Helpers --------------------------------------

def quantize2(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def rand_amount(rng: random.Random) -> Decimal:
    x = rng.lognormvariate(AMOUNT_MU, AMOUNT_SIGMA)
    x = max(float(AMOUNT_MIN), min(float(AMOUNT_MAX), x))
    return Decimal(str(round(x, 2)))


def rand_payout_amount(rng: random.Random) -> Decimal:
    x = rng.lognormvariate(PAYOUT_AMOUNT_MU, PAYOUT_AMOUNT_SIGMA)
    x = max(float(PAYOUT_AMOUNT_MIN), min(float(PAYOUT_AMOUNT_MAX), x))
    return Decimal(str(round(x, 2)))


def pick_card_brand(rng: random.Random):
    r = rng.random()
    cum = 0.0
    for entry in CARD_BRANDS:
        cum += entry[4]
        if r <= cum:
            return entry
    return CARD_BRANDS[-1]


def hex_token(rng: random.Random, length: int) -> str:
    return "".join(rng.choices("0123456789ABCDEF", k=length))


def alnum_token(rng: random.Random, length: int) -> str:
    return "".join(rng.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=length))


def datetime_in_hour(d: date, hour: int, rng: random.Random) -> datetime:
    return datetime(d.year, d.month, d.day, hour,
                    rng.randint(0, 59),
                    rng.randint(0, 59),
                    rng.randint(0, 999_999))


def fmt_date_ddmmyyyy(d: date) -> str: return d.strftime("%d%m%Y")
def fmt_date_ddmmyy(d: date)  -> str: return d.strftime("%d%m%y")
def fmt_date_yymmdd(d: date)  -> str: return d.strftime("%y%m%d")
def fmt_date_yymm(d: date)    -> str: return d.strftime("%y%m")
def fmt_date_yyyymmdd(d: date) -> str: return d.strftime("%Y%m%d")
def fmt_date_iso(d: date)     -> str: return d.strftime("%Y-%m-%d")
def fmt_dt_full(dt: datetime) -> str: return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_dt_micro(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond:06d}"


def fmt_bank_date(d: date) -> str:
    """Bank statement format: 'Mar 25, 2026'."""
    return d.strftime("%b %d, %Y")


def derive_txn_counts() -> dict:
    """Split N_LIFE3_TOTAL between merchants by MERCHANT_WEIGHTS."""
    counts = {m: int(round(N_LIFE3_TOTAL * w)) for m, w in MERCHANT_WEIGHTS.items()}
    drift = N_LIFE3_TOTAL - sum(counts.values())
    if drift != 0:
        first_m = next(iter(counts))
        counts[first_m] += drift
    return counts


def derive_run_dates() -> list:
    """List of calendar days in scope, oldest first, ending on RUN_DATE_END."""
    return [RUN_DATE_END - timedelta(days=i) for i in range(N_RUN_DAYS - 1, -1, -1)]


def derive_merchant_hours() -> dict:
    """Assign active hours per day to each merchant, auto-tuned to
    BALANCE_TO_LIFE3_RATIO and overlapped per MERCHANT_HOUR_OVERLAP.

    Each merchant gets ``counts[m]`` distinct hours/day; the total across
    merchants (== balanceAdjustment buckets/day) is independent of overlap, so
    the ratio holds. overlap=0 -> disjoint windows (legacy); overlap=1 -> all
    merchants start at the same hour. A merchant's own hours stay distinct, so
    (date, balance_account, hour) is still unique -- only cross-merchant hours
    collide.
    """
    target_balance = max(1, round(N_LIFE3_TOTAL * BALANCE_TO_LIFE3_RATIO))
    n_merchants = len(MERCHANT_WEIGHTS)
    available   = ACTIVE_HOUR_END - ACTIVE_HOUR_START
    n_hours_total = max(n_merchants, round(target_balance / N_RUN_DAYS))
    n_hours_total = min(n_hours_total, available)

    counts = {m: max(1, int(round(w * n_hours_total))) for m, w in MERCHANT_WEIGHTS.items()}
    drift = n_hours_total - sum(counts.values())
    if drift != 0:
        biggest = max(counts, key=counts.get)
        counts[biggest] = max(1, counts[biggest] + drift)

    overlap = min(1.0, max(0.0, MERCHANT_HOUR_OVERLAP))

    # Disjoint start = running offset; overlap pulls every start toward 0.
    starts, cumulative = {}, 0
    for m in MERCHANT_WEIGHTS:
        starts[m] = int(round((1.0 - overlap) * cumulative))
        cumulative += counts[m]

    pool_size = min(max(starts[m] + counts[m] for m in MERCHANT_WEIGHTS), available)

    out = {}
    for m in MERCHANT_WEIGHTS:
        s = starts[m] % pool_size
        out[m] = sorted({ACTIVE_HOUR_START + (s + k) % pool_size
                         for k in range(min(counts[m], pool_size))})
    return out


def derive_n_batches() -> dict:
    """Auto-compute Adyen batch variety per merchant (cosmetic).

    Only affects how many distinct settlementBatch values appear in the
    Adyen Settlement file.
    """
    if N_BATCHES_PER_MERCHANT_OVERRIDE is not None:
        return dict(N_BATCHES_PER_MERCHANT_OVERRIDE)
    txn_counts = derive_txn_counts()
    out = {}
    for m, n in txn_counts.items():
        out[m] = max(1, int(round(n * BALANCE_TO_LIFE3_RATIO / 10)))
    return out


# ---------------------------- Data model ----------------------------------

@dataclass
class Txn:
    seq_global:       int
    seq_in_merchant:  int
    merchant:         str
    profile_id:       str
    balance_account:  str
    amount:           Decimal
    currency:         str
    card_brand:       str
    card_pm:          str
    card_variant:     str
    card_pan:         str
    card_holder:      str
    customer_id:      str
    creation_dt:      datetime
    adyen_batch:      int
    life3_batch:      str
    gdpp_trans_desc:  str
    payment_id:       str
    attempt_id:       str
    connector_transaction_id: str
    payment_method_id: str
    card_token:       str
    customer_ref:     str
    fingerprint_id:   str
    sequence_life3:   str
    dd_num:           str
    merchant_reference: str
    modification_reference: str

    # txn type discriminator
    txn_type: str = "payment"  # "payment" | "refund" | "payout"

    # Refund-specific (only set when txn_type == "refund")
    refund_id:           Optional[str] = None  # appears in Life3.GDPP_TRANS_DESC; matches gdpp_refunds.refund_id
    connector_refund_id: Optional[str] = None  # matches Adyen Settlement.Modification Reference
    original_payment_id: Optional[str] = None  # link to the payment being refunded
    refund_status:       str = "success"

    # Payout-specific (only set when txn_type == "payout")
    payout_id:           Optional[str] = None  # appears in Life3.GDPP_TRANS_DESC; matches gdpp_payouts.payout_id
    connector_payout_id: Optional[str] = None  # matches Balance Platform cardTransfer.Transfer Id
    payout_attempt_id:   Optional[str] = None
    payout_status:       str = "success"

    # mismatch flags
    skip_profile:     bool = False
    drop_from_gdpp:   bool = False
    drop_from_adyen:  bool = False
    gdpp_amount_override: Optional[Decimal] = None
    gdpp_status:      str = "charged"

    @property
    def in_gdpp(self) -> bool:
        return not (self.skip_profile or self.drop_from_gdpp)

    @property
    def in_adyen(self) -> bool:
        # Payouts don't appear in Adyen Settlement Detail Report at all.
        if self.txn_type == "payout":
            return False
        if not self.in_gdpp or self.drop_from_adyen:
            return False
        # Payments need GDPP status "charged"; refunds need refund_status "success".
        if self.txn_type == "payment":
            return self.gdpp_status == "charged"
        if self.txn_type == "refund":
            return self.refund_status == "success"
        return False

    @property
    def life3_signed_amount(self) -> Decimal:
        """Amount as it should appear in Life3 (positive for payment, negative for refund/payout)."""
        if self.txn_type == "payment":
            return self.amount
        return -self.amount


# ---------------------------- Build phase ---------------------------------

def _split_by_type(total: int) -> dict:
    """Split a per-merchant total across TXN_TYPE_MIX. Drift goes to payments."""
    out = {tt: int(round(total * w)) for tt, w in TXN_TYPE_MIX.items()}
    drift = total - sum(out.values())
    if drift != 0:
        out["payment"] += drift
    return out


def build_txns(rng: random.Random):
    """Generate transactions in 3 passes: payments, refunds, payouts.

    Pass 1 (payments) defines the universe. Pass 2 (refunds) selects a random
    payment per refund and dates it REFUND_LAG_DAYS_MIN..MAX days later
    (clamped to RUN_DATE_END). Pass 3 (payouts) is independent and inherits
    only merchant/balance_account.
    """
    txn_counts      = derive_txn_counts()
    n_batches       = derive_n_batches()
    merchant_hours  = derive_merchant_hours()
    run_dates       = derive_run_dates()

    txns = []
    seq_global = 0
    payments_by_merchant: dict[str, list[Txn]] = defaultdict(list)

    type_counts = {m: _split_by_type(c) for m, c in txn_counts.items()}

    # ---- Helpers (closures over the loop state) -----------------------------
    def common_card_fields():
        brand_disp, brand_pm, brand_variant, pan_prefix, _ = pick_card_brand(rng)
        pan = pan_prefix + "".join(rng.choices("0123456789", k=10))
        return brand_disp, brand_pm, brand_variant, pan

    def common_mismatch_flags():
        skip_profile    = rng.random() < MISMATCH["skip_profile_rate"]
        drop_from_gdpp  = (not skip_profile) and rng.random() < MISMATCH["missing_in_gdpp_rate"]
        status_failed   = (not skip_profile and not drop_from_gdpp) and rng.random() < MISMATCH["status_failed_rate"]
        drop_from_adyen = (not skip_profile and not drop_from_gdpp and not status_failed) \
                          and rng.random() < MISMATCH["missing_in_adyen_rate"]
        amount_mismatch = rng.random() < MISMATCH["amount_mismatch_rate"]
        return skip_profile, drop_from_gdpp, status_failed, drop_from_adyen, amount_mismatch

    # ---- PASS 1: Payments ---------------------------------------------------
    for merchant, types in type_counts.items():
        meta  = MERCHANT_META[merchant]
        hours = merchant_hours[merchant]
        for seq_in in range(1, types["payment"] + 1):
            seq_global += 1
            run_date    = rng.choice(run_dates)
            hour        = rng.choice(hours)
            creation_dt = datetime_in_hour(run_date, hour, rng)
            day_idx = (run_date - run_dates[0]).days
            adyen_batch = meta["adyen_batch_base"] + day_idx % n_batches[merchant]
            amount      = rand_amount(rng)
            brand_disp, brand_pm, brand_variant, pan = common_card_fields()
            skip_profile, drop_from_gdpp, status_failed, drop_from_adyen, amount_mismatch \
                = common_mismatch_flags()

            profile_id = OFF_SCOPE_PROFILE_ID if skip_profile else rng.choice(meta["profile_ids"])
            gdpp_status = "failed" if status_failed else "charged"
            gdpp_amt_override = quantize2(amount + Decimal("0.01")) if amount_mismatch else None

            row_yymmdd = fmt_date_yymmdd(run_date)
            row_yymm   = fmt_date_yymm(run_date)
            seq5 = f"{seq_in:05d}"

            gdpp_trans_desc = (
                f"{meta['first_letter']}{seq5}{row_yymmdd}{row_yymm}"
                f"{meta['k_code']}{meta['life3_batch']}{RUN_NONCE}"
            )
            payment_id        = gdpp_trans_desc
            attempt_id        = f"{payment_id}_1"
            payment_method_id = (
                f"pm_{meta['first_letter']}{seq5}{row_yymmdd}{row_yymm}"
                f"{meta['k_code']}{RUN_NONCE}"
            )
            card_token        = f"{meta['card_token_prefix']}{seq_in % 100:02d}{row_yymmdd}{seq_in:05d}"
            brand_letter      = "V" if brand_disp == "Visa" else "M"
            connector_txn_id  = f"ZHTPSP{meta['first_letter']}{seq_in:05d}{brand_letter}MIT{row_yymmdd}{RUN_NONCE}"
            merchant_reference     = f"{meta['merchant_ref_prefix']}{row_yymmdd}{hour:02d}{seq_in:07d}_1{RUN_NONCE}"
            modification_reference = f"MOD{seq_in:04d}{row_yymmdd}XXXX"

            t = Txn(
                seq_global=seq_global, seq_in_merchant=seq_in, merchant=merchant,
                profile_id=profile_id, balance_account=meta["balance_account"],
                amount=amount, currency=DEFAULT_CURRENCY,
                card_brand=brand_disp, card_pm=brand_pm, card_variant=brand_variant,
                card_pan=pan, card_holder=f"Customer {seq_global}",
                customer_id=f"cust_zurich_au_{seq_in:05d}",
                creation_dt=creation_dt, adyen_batch=adyen_batch,
                life3_batch=meta["life3_batch"], gdpp_trans_desc=gdpp_trans_desc,
                payment_id=payment_id, attempt_id=attempt_id,
                connector_transaction_id=connector_txn_id,
                payment_method_id=payment_method_id, card_token=card_token,
                customer_ref=hex_token(rng, 64),
                fingerprint_id="fp_" + hex_token(rng, 16).lower(),
                sequence_life3=f"{seq_in:06d}",
                dd_num=f"D5{seq_global:07d}",
                merchant_reference=merchant_reference,
                modification_reference=modification_reference,
                txn_type="payment",
                skip_profile=skip_profile, drop_from_gdpp=drop_from_gdpp,
                drop_from_adyen=drop_from_adyen,
                gdpp_amount_override=gdpp_amt_override,
                gdpp_status=gdpp_status,
            )
            txns.append(t)
            if not skip_profile:
                payments_by_merchant[merchant].append(t)

    # ---- PASS 2: Refunds ----------------------------------------------------
    for merchant, types in type_counts.items():
        meta  = MERCHANT_META[merchant]
        hours = merchant_hours[merchant]
        candidates = payments_by_merchant[merchant]
        if not candidates:
            continue
        for seq_in in range(1, types["refund"] + 1):
            seq_global += 1
            original = rng.choice(candidates)
            # Refund created [LAG_MIN, LAG_MAX] days after original payment.
            lag_days = rng.randint(REFUND_LAG_DAYS_MIN, REFUND_LAG_DAYS_MAX)
            refund_date = min(
                original.creation_dt.date() + timedelta(days=lag_days),
                RUN_DATE_END,
            )
            hour        = rng.choice(hours)
            creation_dt = datetime_in_hour(refund_date, hour, rng)
            day_idx = (refund_date - run_dates[0]).days
            adyen_batch = meta["adyen_batch_base"] + day_idx % n_batches[merchant]
            # Refund amount: typically the full original payment, sometimes partial.
            amount = original.amount if rng.random() < 0.85 else \
                     quantize2(original.amount * Decimal(str(rng.uniform(0.10, 0.99))))
            skip_profile, drop_from_gdpp, status_failed, drop_from_adyen, amount_mismatch \
                = common_mismatch_flags()

            profile_id = OFF_SCOPE_PROFILE_ID if skip_profile else rng.choice(meta["profile_ids"])
            gdpp_status = "success"  # refunds use "success" not "charged"; engine config maps it
            gdpp_amt_override = quantize2(amount + Decimal("0.01")) if amount_mismatch else None

            row_yymmdd = fmt_date_yymmdd(refund_date)
            row_yymm   = fmt_date_yymm(refund_date)
            seq5 = f"{seq_in:05d}"

            # Refund id appears in Life3.GDPP_TRANS_DESC -- distinct id space from payments.
            refund_id = (
                f"{meta['first_letter']}R{seq5}{row_yymmdd}{row_yymm}"
                f"{meta['k_code']}{meta['life3_batch']}{RUN_NONCE}"
            )
            connector_refund_id = f"ZHTPSPR{meta['first_letter']}{seq_in:05d}{row_yymmdd}{RUN_NONCE}"
            attempt_id          = f"{refund_id}_1"
            merchant_reference  = f"{meta['merchant_ref_prefix']}R{row_yymmdd}{hour:02d}{seq_in:07d}_1{RUN_NONCE}"

            t = Txn(
                seq_global=seq_global, seq_in_merchant=seq_in, merchant=merchant,
                profile_id=profile_id, balance_account=meta["balance_account"],
                amount=amount, currency=DEFAULT_CURRENCY,
                # Inherit card/customer details from original payment for realism.
                card_brand=original.card_brand, card_pm=original.card_pm,
                card_variant=original.card_variant, card_pan=original.card_pan,
                card_holder=original.card_holder, customer_id=original.customer_id,
                creation_dt=creation_dt, adyen_batch=adyen_batch,
                life3_batch=meta["life3_batch"],
                gdpp_trans_desc=refund_id,                 # Life3 stores refund_id here
                payment_id=original.payment_id,            # gdpp_refunds.payment_id links to original
                attempt_id=attempt_id,
                connector_transaction_id=original.connector_transaction_id,  # refund's psp_ref = original payment psp_ref
                payment_method_id=original.payment_method_id,
                card_token=original.card_token,
                customer_ref=hex_token(rng, 64),
                fingerprint_id=original.fingerprint_id,
                sequence_life3=f"{seq_in:06d}",
                dd_num=f"D5R{seq_global:06d}",
                merchant_reference=merchant_reference,
                modification_reference=connector_refund_id,
                txn_type="refund",
                refund_id=refund_id,
                connector_refund_id=connector_refund_id,
                original_payment_id=original.payment_id,
                refund_status="success" if not status_failed else "failure",
                skip_profile=skip_profile, drop_from_gdpp=drop_from_gdpp,
                drop_from_adyen=drop_from_adyen,
                gdpp_amount_override=gdpp_amt_override,
                gdpp_status=gdpp_status,
            )
            txns.append(t)

    # ---- PASS 3: Payouts ----------------------------------------------------
    for merchant, types in type_counts.items():
        meta  = MERCHANT_META[merchant]
        hours = merchant_hours[merchant]
        for seq_in in range(1, types["payout"] + 1):
            seq_global += 1
            run_date    = rng.choice(run_dates)
            hour        = rng.choice(hours)
            creation_dt = datetime_in_hour(run_date, hour, rng)
            amount      = rand_payout_amount(rng)
            brand_disp, brand_pm, brand_variant, pan = common_card_fields()
            skip_profile, drop_from_gdpp, _, _, amount_mismatch = common_mismatch_flags()

            profile_id = OFF_SCOPE_PROFILE_ID if skip_profile else rng.choice(meta["profile_ids"])
            gdpp_status = "success"
            gdpp_amt_override = quantize2(amount + Decimal("0.01")) if amount_mismatch else None

            row_yymmdd = fmt_date_yymmdd(run_date)
            row_yymm   = fmt_date_yymm(run_date)
            seq5 = f"{seq_in:05d}"

            payout_id = (
                f"{meta['first_letter']}P{seq5}{row_yymmdd}{row_yymm}"
                f"{meta['k_code']}{meta['life3_batch']}{RUN_NONCE}"
            )
            connector_payout_id = f"EVJN42PAYOUT{row_yymmdd}{seq_in:05d}{merchant[-3:]}{RUN_NONCE}"
            payout_attempt_id   = f"{payout_id}_1"
            payment_method_id   = f"pm_PAYOUT{seq_in % 100:02d}{row_yymmdd}{seq_in:05d}{RUN_NONCE}"
            card_token          = f"pm_PAYOUTCARD{seq_in % 100:02d}{row_yymmdd}{seq_in:05d}{RUN_NONCE}"

            t = Txn(
                seq_global=seq_global, seq_in_merchant=seq_in, merchant=merchant,
                profile_id=profile_id, balance_account=meta["balance_account"],
                amount=amount, currency=DEFAULT_CURRENCY,
                card_brand=brand_disp, card_pm=brand_pm, card_variant=brand_variant,
                card_pan=pan, card_holder=f"Payee {seq_global}",
                customer_id=f"cust_zurich_au_payout_{seq_in:05d}",
                creation_dt=creation_dt, adyen_batch=0,    # not in Adyen settlement
                life3_batch=meta["life3_batch"],
                gdpp_trans_desc=payout_id,                 # Life3 stores payout_id here
                payment_id=payout_id,                       # not used; kept for shape
                attempt_id=payout_attempt_id,
                connector_transaction_id="",               # no Adyen settlement psp_ref
                payment_method_id=payment_method_id,
                card_token=card_token,
                customer_ref=hex_token(rng, 64),
                fingerprint_id="fp_" + hex_token(rng, 16).lower(),
                sequence_life3=f"{seq_in:06d}",
                dd_num=f"D5P{seq_global:06d}",
                merchant_reference=f"{meta['merchant_ref_prefix']}P{row_yymmdd}{hour:02d}{seq_in:07d}_1{RUN_NONCE}",
                modification_reference="",
                txn_type="payout",
                payout_id=payout_id,
                connector_payout_id=connector_payout_id,
                payout_attempt_id=payout_attempt_id,
                payout_status="success",
                skip_profile=skip_profile, drop_from_gdpp=drop_from_gdpp,
                drop_from_adyen=False,                      # n/a for payouts
                gdpp_amount_override=gdpp_amt_override,
                gdpp_status=gdpp_status,
            )
            txns.append(t)

    # ---- PASS 4: Forced negative drains -------------------------------------
    # Synthesize oversized "chargeback" refunds in N drain windows per balance
    # account so the bank statement has Debit rows. Each synthetic refund is
    # linked to an existing payment (so the refund<->payment chain still has a
    # parent), but its amount is sized to flip its drain window net-negative.
    txns.extend(_build_chargeback_refunds(txns, rng))

    return txns


def _build_chargeback_refunds(txns, rng: random.Random):
    """Return a list of synthetic refund Txns that force N drain windows
    per balance account to net negative.

    Strategy: simulate the per-(date, hour) bucket aggregation that
    aggregate_to_balance() will perform, chunk the buckets into drain
    windows the same way make_bank_transfers() does, pick N drain windows
    per balance account, and inject one large refund per chosen window
    sized to flip its net.
    """
    if N_FORCED_NEGATIVE_DRAINS_PER_BA <= 0:
        return []

    run_dates = derive_run_dates()
    n_batches = derive_n_batches()

    # Group adyen-bound txns by balance_account; build per-BA bucket sums and
    # a pool of parent payments to attach synthetic refunds to.
    by_ba_buckets:  dict = defaultdict(lambda: defaultdict(lambda: Decimal("0.00")))
    by_ba_parents:  dict = defaultdict(list)
    for t in txns:
        if not t.in_adyen:
            continue
        sign = Decimal(1) if t.txn_type == "payment" else Decimal(-1)
        by_ba_buckets[t.balance_account][(t.creation_dt.date(), t.creation_dt.hour)] += sign * t.amount
        if t.txn_type == "payment":
            by_ba_parents[t.balance_account].append(t)

    extra: list = []
    syn_seq = 0

    for ba, buckets in by_ba_buckets.items():
        parents = by_ba_parents.get(ba)
        if not parents or not buckets:
            continue
        merchant = next(
            (m for m, mm in MERCHANT_META.items() if mm["balance_account"] == ba),
            None,
        )
        if merchant is None:
            continue
        meta = MERCHANT_META[merchant]

        sorted_keys = sorted(buckets.keys())  # (date, hour) by booking order
        chunks = [
            sorted_keys[i:i + DRAIN_GROUP_SIZE]
            for i in range(0, len(sorted_keys), DRAIN_GROUP_SIZE)
        ]
        # Skip undersized tail chunks so the forced negative is meaningful.
        viable = [c for c in chunks if len(c) >= max(2, DRAIN_GROUP_SIZE // 2)]
        if not viable:
            continue

        n_force = min(N_FORCED_NEGATIVE_DRAINS_PER_BA, len(viable))
        chosen  = rng.sample(viable, n_force)

        for chunk in chosen:
            drain_net = sum((buckets[k] for k in chunk), Decimal("0.00"))
            if drain_net <= 0:
                continue  # already negative -- nothing to force
            multiplier   = Decimal(str(rng.uniform(
                CHARGEBACK_MULTIPLIER_MIN, CHARGEBACK_MULTIPLIER_MAX
            )))
            synth_amount = quantize2(drain_net * multiplier)
            if synth_amount <= 0:
                continue

            tgt_date, tgt_hour = rng.choice(chunk)
            creation_dt        = datetime_in_hour(tgt_date, tgt_hour, rng)
            parent             = rng.choice(parents)

            syn_seq += 1
            day_idx     = (tgt_date - run_dates[0]).days
            adyen_batch = meta["adyen_batch_base"] + day_idx % n_batches[merchant]
            row_yymmdd  = fmt_date_yymmdd(tgt_date)
            row_yymm    = fmt_date_yymm(tgt_date)
            seq5        = f"{syn_seq:05d}"

            # "C" (chargeback) marker keeps these IDs disjoint from the
            # "R" (refund) ids generated in PASS 2.
            refund_id           = (
                f"{meta['first_letter']}C{seq5}{row_yymmdd}{row_yymm}"
                f"{meta['k_code']}{meta['life3_batch']}{RUN_NONCE}"
            )
            connector_refund_id = f"ZHTPSPC{meta['first_letter']}{syn_seq:05d}{row_yymmdd}{RUN_NONCE}"
            attempt_id          = f"{refund_id}_1"
            merchant_reference  = (
                f"{meta['merchant_ref_prefix']}C{row_yymmdd}{tgt_hour:02d}{syn_seq:07d}_1{RUN_NONCE}"
            )

            extra.append(Txn(
                seq_global=900000 + syn_seq,
                seq_in_merchant=900000 + syn_seq,
                merchant=merchant,
                profile_id=rng.choice(meta["profile_ids"]),
                balance_account=ba,
                amount=synth_amount,
                currency=DEFAULT_CURRENCY,
                card_brand=parent.card_brand, card_pm=parent.card_pm,
                card_variant=parent.card_variant, card_pan=parent.card_pan,
                card_holder=parent.card_holder, customer_id=parent.customer_id,
                creation_dt=creation_dt, adyen_batch=adyen_batch,
                life3_batch=meta["life3_batch"],
                gdpp_trans_desc=refund_id,
                payment_id=parent.payment_id,
                attempt_id=attempt_id,
                connector_transaction_id=parent.connector_transaction_id,
                payment_method_id=parent.payment_method_id,
                card_token=parent.card_token,
                customer_ref=hex_token(rng, 64),
                fingerprint_id=parent.fingerprint_id,
                sequence_life3=f"{900000 + syn_seq:06d}",
                dd_num=f"D5C{syn_seq:07d}",
                merchant_reference=merchant_reference,
                modification_reference=connector_refund_id,
                txn_type="refund",
                refund_id=refund_id,
                connector_refund_id=connector_refund_id,
                original_payment_id=parent.payment_id,
                refund_status="success",
                skip_profile=False, drop_from_gdpp=False, drop_from_adyen=False,
                gdpp_amount_override=None, gdpp_status="success",
            ))
    return extra


# ---------------------------- Writers -------------------------------------

LIFE3_HEADERS = [
    "TRANSACTION TYPE", "SEQUENCE", "MERC NUM OLD", "DD NUM", "CARD NUM",
    "EXP DATE", "TRANSACTION DATE", "TRANSACTION AMOUNT", "CARD TYPE",
    "FILL1", "TOKEN", "CUSTOMER-REF", "GDPP MERC NUM", "GDPP TRANS DESC",
    "BATCH",
]


def write_life3(txns, path: str):
    """Life3 file: header + metadata row + data rows + footer.
    All rows have a trailing comma (the file is fixed-width origin).

    TRANSACTION AMOUNT is unsigned 8 digits for payments (e.g. '00008700')
    and 8 chars including a leading minus for refunds + payouts (e.g.
    '-0008700'). The engine config decides which type each row is via the
    sign of the amount; no separate type column is needed.
    """
    with open(path, "w", newline="") as f:
        f.write(",".join(LIFE3_HEADERS) + ",\n")
        f.write(f"100000{1:01d}{fmt_date_yyyymmdd(RUN_DATE_END)}\n")

        sorted_txns = sorted(txns, key=lambda t: (t.creation_dt, t.merchant, t.seq_in_merchant))

        total_cents = 0
        count = 0
        for t in sorted_txns:
            signed_amount = t.life3_signed_amount
            cents = int((signed_amount * 100).to_integral_value())
            total_cents += cents
            count += 1
            if cents >= 0:
                amount_str = f"{cents:08d}"
            else:
                amount_str = f"-{abs(cents):07d}"
            row = [
                "3",
                t.sequence_life3,
                "",
                t.dd_num,
                t.card_pan,
                "",
                fmt_date_ddmmyyyy(t.creation_dt.date()),
                amount_str,
                "G",
                "",
                t.card_token,
                t.customer_ref,
                t.profile_id,
                t.gdpp_trans_desc,
                t.life3_batch,
            ]
            f.write(",".join(row) + ",\n")
        # Footer: signed grand total. Match the per-row width convention.
        if total_cents >= 0:
            total_str = f"{total_cents:018d}"
        else:
            total_str = f"-{abs(total_cents):017d}"
        f.write(f"6{count:06d}{total_str}C\n")
    return count


GDPP_PAYMENT_HEADERS = [
    "payment_id", "attempt_id", "status", "amount", "currency", "connector",
    "connector_transaction_id", "amount_to_capture", "customer_id", "created_at",
    "order_details", "error_message", "capture_method", "authentication_type",
    "mandate_id", "payment_method", "payment_method_type", "metadata",
    "setup_future_usage", "statement_descriptor_name", "description",
    "off_session", "business_country", "business_label", "business_sub_label",
    "allowed_payment_method_types", "payment_method_data", "card_network",
    "fingerprint_id", "modified_at", "error_code", "payment_method_id",
    "card_holder_name", "merchant_order_reference_id", "profile_id",
]


def write_gdpp_payments(txns, path: str):
    """GDPP payments file -- payment-type rows only, all amounts positive."""
    rows = [t for t in txns if t.txn_type == "payment" and t.in_gdpp]
    rows.sort(key=lambda t: t.creation_dt)
    with open(path, "w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(GDPP_PAYMENT_HEADERS)
        for t in rows:
            amount = t.gdpp_amount_override if t.gdpp_amount_override is not None else t.amount
            payment_method_data = json.dumps({
                "card": {
                    "last4": t.card_pan[-4:],
                    "card_type": "CREDIT",
                    "card_issuer": "ZURICH AU BANK",
                    "card_network": t.card_brand,
                    "card_exp_year": "2067",
                    "card_exp_month": "05",
                    "card_holder_name": t.card_holder,
                    "card_issuing_country": "AUSTRALIA",
                    "card_issuing_country_code": "AU",
                }
            }).replace('"', "'")
            modified_dt = t.creation_dt + timedelta(seconds=2)
            w.writerow([
                t.payment_id,
                t.attempt_id,
                t.gdpp_status,
                f"{amount:.2f}",
                t.currency,
                "adyen",
                t.connector_transaction_id,
                "",
                t.customer_id,
                fmt_dt_micro(t.creation_dt),
                "",
                "",
                "automatic",
                "three_ds",
                "",
                "card",
                "credit",
                "",
                "",
                "",
                "Zurich AU MIT Payment",
                "false",
                "AU",
                t.merchant,
                "",
                "",
                payment_method_data,
                t.card_brand,
                t.fingerprint_id,
                fmt_dt_micro(modified_dt),
                "",
                t.payment_method_id,
                t.card_holder,
                "",
                t.profile_id,
            ])
    return len(rows)


GDPP_REFUND_HEADERS = [
    "internal_reference_id", "refund_id", "payment_id", "merchant_id",
    "connector_transaction_id", "connector", "total_amount", "currency",
    "refund_amount", "refund_status", "connector_refund_id",
    "external_reference_id", "refund_reason", "refund_type", "sent_to_gateway",
    "refund_error_message", "metadata", "created_at", "modified_at",
    "description", "attempt_id", "refund_error_code", "profile_id",
]


def write_gdpp_refunds(txns, path: str):
    """GDPP refunds file -- refund-type rows only, all amounts positive."""
    rows = [t for t in txns if t.txn_type == "refund" and t.in_gdpp]
    rows.sort(key=lambda t: t.creation_dt)
    with open(path, "w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(GDPP_REFUND_HEADERS)
        for t in rows:
            refund_amt = t.gdpp_amount_override if t.gdpp_amount_override is not None else t.amount
            modified_dt = t.creation_dt + timedelta(seconds=2)
            metadata = json.dumps({
                "merchant_account": t.merchant,
                "card_last4":       t.card_pan[-4:],
            }).replace('"', "'")
            w.writerow([
                f"int_ref_{t.seq_global:08d}",          # internal_reference_id
                t.refund_id,                             # refund_id (Hop 1 key)
                t.original_payment_id,                  # payment_id (link to original)
                t.merchant,                              # merchant_id
                t.connector_transaction_id,             # connector_transaction_id (= original payment psp_ref)
                "adyen",                                 # connector
                f"{refund_amt:.2f}",                     # total_amount (full original amount, simplified)
                t.currency,                              # currency
                f"{refund_amt:.2f}",                     # refund_amount (positive)
                t.refund_status,                         # refund_status
                t.connector_refund_id,                  # connector_refund_id (Hop 2b key)
                "",                                      # external_reference_id
                "REQUESTED_BY_CUSTOMER",                 # refund_reason
                "instant",                               # refund_type
                "true",                                  # sent_to_gateway
                "",                                      # refund_error_message
                metadata,                                # metadata
                fmt_dt_micro(t.creation_dt),            # created_at
                fmt_dt_micro(modified_dt),              # modified_at
                "Zurich AU Refund",                      # description
                t.attempt_id,                            # attempt_id
                "",                                      # refund_error_code
                t.profile_id,                            # profile_id
            ])
    return len(rows)


GDPP_PAYOUT_HEADERS = [
    "payout_id", "payout_attempt_id", "payout_link_id",
    "merchant_order_reference_id", "connector_payout_id", "status", "amount",
    "currency", "payout_type", "confirm", "attempt_count", "is_eligible",
    "connector", "payout_method_id", "profile_id", "merchant_id",
    "organization_id", "customer_id", "recurring", "auto_fulfill", "priority",
    "description", "error_code", "error_message", "unified_code",
    "unified_message", "business_country", "business_label", "entity_type",
    "created_at", "last_modified_at", "additional_payout_method_data",
    "metadata",
]


def write_gdpp_payouts(txns, path: str):
    """GDPP payouts file -- payout-type rows only, all amounts positive."""
    rows = [t for t in txns if t.txn_type == "payout" and t.in_gdpp]
    rows.sort(key=lambda t: t.creation_dt)
    with open(path, "w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(GDPP_PAYOUT_HEADERS)
        for t in rows:
            payout_amt = t.gdpp_amount_override if t.gdpp_amount_override is not None else t.amount
            modified_dt = t.creation_dt + timedelta(seconds=2)
            additional_payout_method_data = json.dumps({
                "card": {
                    "last4":          t.card_pan[-4:],
                    "card_network":   t.card_brand,
                    "card_holder":    t.card_holder,
                    "card_exp_month": "05",
                    "card_exp_year":  "2067",
                }
            }).replace('"', "'")
            metadata = json.dumps({
                "merchant_account": t.merchant,
                "purpose":          "claim_payout",
            }).replace('"', "'")
            w.writerow([
                t.payout_id,                             # payout_id (Hop 1 key)
                t.payout_attempt_id,                    # payout_attempt_id
                "",                                      # payout_link_id
                t.merchant_reference,                    # merchant_order_reference_id
                t.connector_payout_id,                  # connector_payout_id (Hop 3b key)
                t.payout_status,                         # status
                f"{payout_amt:.2f}",                     # amount (positive)
                t.currency,                              # currency
                "card",                                  # payout_type
                "true",                                  # confirm
                "1",                                     # attempt_count
                "true",                                  # is_eligible
                "adyen",                                 # connector
                t.payment_method_id,                    # payout_method_id
                t.profile_id,                            # profile_id
                t.merchant,                              # merchant_id
                "org_zurich_au",                         # organization_id
                t.customer_id,                           # customer_id
                "false",                                 # recurring
                "true",                                  # auto_fulfill
                "regular",                               # priority
                "Zurich AU Payout",                      # description
                "",                                      # error_code
                "",                                      # error_message
                "",                                      # unified_code
                "",                                      # unified_message
                "AU",                                    # business_country
                t.merchant,                              # business_label
                "individual",                            # entity_type
                fmt_dt_micro(t.creation_dt),            # created_at
                fmt_dt_micro(modified_dt),              # last_modified_at
                additional_payout_method_data,          # additional_payout_method_data
                metadata,                                # metadata
            ])
    return len(rows)


ADYEN_SETTLEMENT_HEADERS = [
    "Company Account", "Merchant Account", "Psp Reference", "Merchant Reference",
    "Payment Method", "Creation Date", "TimeZone", "Type", "Modification Reference",
    "Gross Currency", "Gross Debit (GC)", "Gross Credit (GC)", "Exchange Rate",
    "Net Currency", "Net Debit (NC)", "Net Credit (NC)", "Commission (NC)",
    "Markup (NC)", "Scheme Fees (NC)", "Interchange (NC)", "Payment Method Variant",
    "Modification Merchant Reference", "Batch Number",
    "Reserved4", "Reserved5", "Reserved6", "Reserved7",
    "Reserved8", "Reserved9", "Reserved10",
]


def write_adyen_settlement(txns, path: str):
    """Adyen Settlement Detail report.

    payments -> 'Settled' row with Gross Credit > 0, Gross Debit empty.
    refunds  -> 'Refunded' row with Gross Debit > 0, Gross Credit empty,
                Modification Reference = connector_refund_id (Hop 2b key).
    payouts  -> not in this report.
    """
    rows = [t for t in txns if t.in_adyen]
    rows.sort(key=lambda t: (t.merchant, t.adyen_batch, t.creation_dt))
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(ADYEN_SETTLEMENT_HEADERS)
        for t in rows:
            creation_no_micro = t.creation_dt.replace(microsecond=0)
            if t.txn_type == "payment":
                net_credit = quantize2(t.amount - (FEE_MARKUP + FEE_SCHEME + FEE_INTERCHANGE))
                w.writerow([
                    COMPANY_ACCOUNT_ADYEN,
                    t.merchant,
                    t.connector_transaction_id,    # Psp Reference
                    t.merchant_reference,
                    t.card_pm,
                    fmt_dt_full(creation_no_micro),
                    "CET",
                    "Settled",
                    t.modification_reference,
                    "AUD",
                    "",                             # Gross Debit (GC)
                    f"{t.amount:.2f}",              # Gross Credit (GC)
                    "1.000000000000000",
                    "AUD",
                    "",                             # Net Debit (NC)
                    f"{net_credit:.2f}",
                    "",                             # Commission (NC)
                    f"{FEE_MARKUP:.2f}",
                    f"{FEE_SCHEME:.2f}",
                    f"{FEE_INTERCHANGE:.2f}",
                    t.card_variant,
                    "<auto>",
                    str(t.adyen_batch),
                    "", "", "", "", "", "", "",     # Reserved4-10
                ])
            elif t.txn_type == "refund":
                # Refunds typically don't carry interchange/scheme fees on the
                # settlement side; net debit equals gross debit.
                w.writerow([
                    COMPANY_ACCOUNT_ADYEN,
                    t.merchant,
                    t.connector_transaction_id,    # Psp Reference (= original payment psp_ref)
                    t.merchant_reference,
                    t.card_pm,
                    fmt_dt_full(creation_no_micro),
                    "CET",
                    "Refunded",
                    t.connector_refund_id,         # Modification Reference (Hop 2b key)
                    "AUD",
                    f"{t.amount:.2f}",              # Gross Debit (GC)
                    "",                             # Gross Credit (GC)
                    "1.000000000000000",
                    "AUD",
                    f"{t.amount:.2f}",              # Net Debit (NC)
                    "",                             # Net Credit (NC)
                    "",                             # Commission (NC)
                    "0.00",                         # Markup (NC)
                    "0.00",                         # Scheme Fees (NC)
                    "0.00",                         # Interchange (NC)
                    t.card_variant,
                    t.connector_refund_id,         # Modification Merchant Reference
                    str(t.adyen_batch),
                    "", "", "", "", "", "", "",     # Reserved4-10
                ])
    return len(rows)


BALANCE_HEADERS = [
    "BalancePlatform", "AccountHolder", "BalanceAccount", "Category", "Type",
    "Status", "Transfer Id", "Transaction Id",
    "Psp Payment Merchant Reference", "Psp Payment Psp Reference",
    "Psp Modification Psp Reference", "Psp Modification Merchant Reference",
    "Reference", "Description", "Booking Date", "Booking Date TimeZone",
    "Value Date", "Value Date TimeZone", "Currency", "Amount",
    "Starting Balance Currency", "Starting Balance",
    "Ending Balance Currency", "Ending Balance",
    "Reserved1", "Reserved2", "Reserved3", "Reserved4", "Reserved5",
    "Reserved6", "Reserved7", "Reserved8", "Reserved9", "Reserved10",
]


def aggregate_to_balance(txns, rng: random.Random):
    """Group settlement-bound txns by (date, merchant, creation_hour) into
    balanceAdjustment rows.

    One row per (BalanceAccount, calendar day, hour-of-day). Payment Settled
    rows contribute positively; refund Refunded rows subtract. Booking_dt is
    set to (D, H+1, random_minute, random_second) -- non-zero minute mimics
    the real Adyen sample (e.g. 17:16:00) while hour-truncation on both sides
    of Hop 3 still collapses the bucket cleanly.

    Net amount (``true_sum``) can be negative when refunds exceed payments
    in a bucket; we emit it as-is. It is used both for the Adyen Settlement
    -> Balance Platform invariant and downstream by ``make_bank_transfers``
    (payments and refunds both settle with the bank). Payouts do NOT pass
    through this aggregation; they appear as separate cardTransfer rows on
    Balance Platform and never reach the bank.
    """
    grouped = defaultdict(list)
    for t in txns:
        if not t.in_adyen:
            continue
        key = (t.creation_dt.date(), t.merchant, t.adyen_batch, t.creation_dt.hour)
        grouped[key].append(t)

    adj_rows = []
    for (run_date, merchant, batch, src_hour), bucket_txns in grouped.items():
        true_sum = Decimal("0.00")
        for t in bucket_txns:
            if t.txn_type == "payment":
                true_sum += t.amount
            elif t.txn_type == "refund":
                true_sum -= t.amount
        amount = true_sum
        if rng.random() < MISMATCH["aggregation_off_rate"]:
            amount = quantize2(amount + Decimal("0.01"))
        meta = MERCHANT_META[merchant]
        # batch comes from the grouping key: each balanceAdjustment aggregates
        # exactly the settlement rows sharing (merchant, batch, hour).
        booking_hour = src_hour + 1  # ACTIVE_HOUR_END capped so this <= 23
        booking_dt = datetime(
            run_date.year, run_date.month, run_date.day,
            booking_hour,
            rng.randint(0, 59),
            rng.randint(0, 59),
        )
        row_yymmdd = fmt_date_yymmdd(run_date)
        psp_ref = f"PSPADJ{row_yymmdd}{booking_hour:02d}BA{batch:04d}{RUN_NONCE}"
        adj_rows.append({
            "merchant":              merchant,
            "balance_platform":      BALANCE_PLATFORM_ADYEN,
            "account_holder":        meta["account_holder"],
            "balance_account":       meta["balance_account"],
            "category":              "PlatformPayment",
            "type":                  "balanceAdjustment",
            "status":                "booked",
            "transfer_id":           f"EVJN429TL{row_yymmdd}00H{booking_hour:02d}BATCH{batch}{RUN_NONCE}",
            "transaction_id":        f"TX{row_yymmdd}00000AT{batch}H{booking_hour:02d}{RUN_NONCE}",
            "psp_payment_merchant_reference":      "",
            "psp_payment_psp_reference":           "",
            "psp_modification_psp_reference":      psp_ref,
            "psp_modification_merchant_reference": "",
            "reference_obj": {
                "pspReference":     psp_ref,
                "merchantAccount":  merchant,
                "currencyCode":     "AUD",
                "valueDate":        f"{fmt_date_iso(run_date)} 00:00:00",
                "settlementBatch":  str(batch),
            },
            "booking_dt":            booking_dt,
            "amount":                amount,        # may be tweaked
            "true_sum":              true_sum,      # always pre-tweak
        })
    return adj_rows


def make_card_transfers(txns, rng: random.Random):
    """cardTransfer Balance Platform rows for each payout.

    Hop 3b: gdpp_payouts.connector_payout_id == cardTransfer.Transfer Id.

    Successful payout -> one 'booked' row with Amount = -payout_amount.
    Failed payout     -> two rows sharing the same Transfer Id but with
                         different Transaction Ids:
                           - 'booked', Amount = -payout_amount,
                           - 'fail',   Amount = +payout_amount, booked a
                                       few seconds later (reversal).
    Failure is sampled per payout at PAYOUT_FAIL_RATE. The GDPP/Life3 sides
    are unchanged (single entry per payout regardless of Adyen outcome).

    booking_dt = creation_dt + a few minutes (Adyen typically books the
    cardTransfer shortly after the payout request is initiated).
    """
    payouts = sorted(
        [t for t in txns if t.txn_type == "payout" and t.in_gdpp],
        key=lambda t: t.creation_dt,
    )
    rows = []
    for seq, t in enumerate(payouts, start=1):
        meta = MERCHANT_META[t.merchant]
        booking_dt = t.creation_dt + timedelta(minutes=rng.randint(2, 30))
        cc_ref = f"CC{seq:08d}{RUN_NONCE}"
        row_yymmdd = fmt_date_yymmdd(booking_dt.date())
        booked = {
            "merchant":              t.merchant,
            "balance_platform":      BALANCE_PLATFORM_ADYEN,
            "account_holder":        meta["account_holder"],
            "balance_account":       meta["balance_account"],
            "category":              "PlatformPayment",
            "type":                  "cardTransfer",
            "status":                "booked",
            "transfer_id":           t.connector_payout_id,    # Hop 3b key
            "transaction_id":        f"TXCT{row_yymmdd}{seq:06d}{RUN_NONCE}",
            "psp_payment_merchant_reference":      "",
            "psp_payment_psp_reference":           "",
            "psp_modification_psp_reference":      "",
            "psp_modification_merchant_reference": "",
            "reference_str":   cc_ref,
            "description_str": f"Card transfer to {t.card_holder}",
            "booking_dt":      booking_dt,
            "amount":          -t.amount,            # negative on Adyen side
            "true_sum":        -t.amount,
        }
        rows.append(booked)
        if rng.random() < PAYOUT_FAIL_RATE:
            fail_booking_dt = booking_dt + timedelta(seconds=rng.randint(3, 30))
            fail_yymmdd     = fmt_date_yymmdd(fail_booking_dt.date())
            rows.append({
                **booked,
                "status":         "fail",
                # Different Transaction Id; same Transfer Id (shared with booked).
                "transaction_id": f"TXCTF{fail_yymmdd}{seq:06d}{RUN_NONCE}",
                "booking_dt":     fail_booking_dt,
                "amount":         t.amount,           # positive -- reversal
                "true_sum":       t.amount,
            })
    return rows


def make_bank_transfers(adj_rows):
    """Generate bankTransfer rows by draining each balance account every
    DRAIN_GROUP_SIZE balanceAdjustments. The drain's transfer_dt = booking_dt
    of the last adjustment in the group.

    Payments and refunds both settle with the bank: the drain sums each
    balanceAdjustment's ``true_sum`` (payments minus refunds, since refunds
    are already netted into the bucket). cardTransfer rows (payouts) are NOT
    included -- they remain on Balance Platform but never reach the bank
    file. bankTransfer.Amount = -1 * sum(true_sum) per drain window; usually
    negative (a credit at the bank) but positive when refunds dominate.

    Drains use the pre-tweak ``true_sum`` so the bankTransfer<->bank
    relationship stays consistent even when aggregation_off is set.
    """
    transfers = []
    seq = 1

    by_ba_adj = defaultdict(list)
    for r in adj_rows:
        by_ba_adj[r["balance_account"]].append(r)

    for ba in by_ba_adj.keys():
        merchant = next(
            (m for m, mm in MERCHANT_META.items() if mm["balance_account"] == ba),
            None,
        )
        if merchant is None:
            raise ValueError(f"BalanceAccount {ba} not present in MERCHANT_META")
        meta      = MERCHANT_META[merchant]
        bank_meta = BANK_META[meta["bank"]]
        ba_adj_sorted = sorted(by_ba_adj[ba], key=lambda r: r["booking_dt"])

        # Drain every DRAIN_GROUP_SIZE balanceAdjustments at the group's last
        # adjustment booking_dt. Drains accumulate across calendar days.
        drain_dts = [
            ba_adj_sorted[min(i + DRAIN_GROUP_SIZE - 1, len(ba_adj_sorted) - 1)]
                ["booking_dt"]
            for i in range(0, len(ba_adj_sorted), DRAIN_GROUP_SIZE)
        ]

        prev_cutoff = datetime(1900, 1, 1)
        for transfer_dt in drain_dts:
            hh, mm = transfer_dt.hour, transfer_dt.minute
            row_yymmdd = fmt_date_yymmdd(transfer_dt.date())
            # Sum balanceAdjustment true_sum (payments net of refunds).
            # cardTransfer rows (payouts) do not contribute -- they never
            # reach the bank.
            net = sum(
                (r["true_sum"] for r in ba_adj_sorted
                 if prev_cutoff <= r["booking_dt"] <= transfer_dt),
                Decimal("0.00"),
            )
            batch_first = meta["adyen_batch_base"]
            swpe = f"SWPE{seq:05d}223MXFFQ6P{RUN_NONCE}"
            transfers.append({
                "merchant":              merchant,
                "bank":                  meta["bank"],
                "balance_platform":      BALANCE_PLATFORM_ADYEN,
                "account_holder":        meta["account_holder"],
                "balance_account":       ba,
                "category":              "BankTransfer",
                "type":                  "bankTransfer",
                "status":                "booked",
                "transfer_id":           f"EVJN42DLH{hh:02d}{mm:02d}{row_yymmdd}00{batch_first}{seq:04d}{RUN_NONCE}",
                "transaction_id":        f"TX{row_yymmdd}00000BT{batch_first}T{hh:02d}{mm:02d}{seq:04d}{RUN_NONCE}",
                "psp_payment_merchant_reference":      "",
                "psp_payment_psp_reference":           "",
                "psp_modification_psp_reference":      "",
                "psp_modification_merchant_reference": "",
                "reference_str":   swpe,
                "description_str": (
                    f"Bank transfer to Zurich AU {meta['bank'].capitalize()} - "
                    f"ADYEN-{bank_meta['bank_short']}-{row_yymmdd}{hh:02d}{mm:02d}{seq:04d}"
                ),
                "booking_dt":  transfer_dt + timedelta(seconds=30),
                "amount":      -net,
            })
            seq += 1
            prev_cutoff = transfer_dt + timedelta(seconds=30)
    return transfers


# Realistic narratives for the random unmatched Debit rows. The Tran Type
# decides which narrative + reference prefix is used.
_RANDOM_DEBIT_KINDS = [
    ("BPAY",       "BPAY", "BPAY PAYMENT TO BILLER"),
    ("DEBIT",      "DD",   "DIRECT DEBIT - INSURANCE PREMIUM"),
    ("TRANSFER",   "MAN",  "MANUAL TRANSFER OUT"),
    ("WITHDRAWAL", "WD",   "ATM WITHDRAWAL"),
    ("FEE",        "FEE",  "MONTHLY ACCOUNT FEE"),
]


def make_random_bank_debits(rng: random.Random):
    """Standalone Debit rows for the bank statement.

    These rows have no counterpart in Adyen Settlement / Balance Platform
    (no bankTransfer, no balanceAdjustment). They simulate unrelated bank
    activity (BPAY, direct debits, fees, withdrawals, manual transfers) and
    are there so the recon engine has unmatched rows to skip.

    N_RANDOM_BANK_DEBITS rows are emitted PER BANK so each bank's statement
    section has its own unmatched activity.
    """
    run_dates = derive_run_dates()
    rows = []
    for bank in BANK_META:
        for _ in range(N_RANDOM_BANK_DEBITS):
            run_date  = rng.choice(run_dates)
            booking_dt = datetime(
                run_date.year, run_date.month, run_date.day,
                rng.randint(8, 18),
                rng.randint(0, 59),
                rng.randint(0, 59),
            )
            tran_type, ref_prefix, narrative_base = rng.choice(_RANDOM_DEBIT_KINDS)
            amt_f = rng.uniform(float(RANDOM_BANK_DEBIT_MIN), float(RANDOM_BANK_DEBIT_MAX))
            amount = Decimal(str(round(amt_f, 2)))
            reference = f"{ref_prefix}{hex_token(rng, 10)}"
            narrative = f"{narrative_base} - REF {hex_token(rng, 6)}"
            rows.append({
                "booking_dt": booking_dt,
                "bank":       bank,
                "amount":     -amount,    # negative => Debit in the bank file
                "tran_type":  tran_type,
                "reference":  reference,
                "narrative":  narrative,
            })
    return rows


def write_balance_platform(adj_rows, bank_transfer_rows, card_transfer_rows, path: str):
    rows = []
    for r in adj_rows:
        ref_json = json.dumps(r["reference_obj"], separators=(",", ":"))
        row_date = r["booking_dt"].date()
        rows.append([
            r["balance_platform"], r["account_holder"], r["balance_account"],
            r["category"], r["type"], r["status"],
            r["transfer_id"], r["transaction_id"],
            r["psp_payment_merchant_reference"], r["psp_payment_psp_reference"],
            r["psp_modification_psp_reference"], r["psp_modification_merchant_reference"],
            ref_json, ref_json,
            fmt_dt_full(r["booking_dt"]), "CET",
            fmt_date_iso(row_date), "CET",
            "AUD", f"{r['amount']:.2f}",
            "", "", "", "",
            "", "", "", "", "", "", "", "", "", "",
        ])
    for r in bank_transfer_rows:
        row_date = r["booking_dt"].date()
        rows.append([
            r["balance_platform"], r["account_holder"], r["balance_account"],
            r["category"], r["type"], r["status"],
            r["transfer_id"], r["transaction_id"],
            r["psp_payment_merchant_reference"], r["psp_payment_psp_reference"],
            r["psp_modification_psp_reference"], r["psp_modification_merchant_reference"],
            r["reference_str"], r["description_str"],
            fmt_dt_full(r["booking_dt"]), "CET",
            fmt_date_iso(row_date), "CET",
            "AUD", f"{r['amount']:.2f}",
            "", "", "", "",
            "", "", "", "", "", "", "", "", "", "",
        ])
    for r in card_transfer_rows:
        row_date = r["booking_dt"].date()
        rows.append([
            r["balance_platform"], r["account_holder"], r["balance_account"],
            r["category"], r["type"], r["status"],
            r["transfer_id"], r["transaction_id"],
            r["psp_payment_merchant_reference"], r["psp_payment_psp_reference"],
            r["psp_modification_psp_reference"], r["psp_modification_merchant_reference"],
            r["reference_str"], r["description_str"],
            fmt_dt_full(r["booking_dt"]), "CET",
            fmt_date_iso(row_date), "CET",
            "AUD", f"{r['amount']:.2f}",
            "", "", "", "",
            "", "", "", "", "", "", "", "", "", "",
        ])
    rows.sort(key=lambda row: row[14])  # Booking Date

    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(BALANCE_HEADERS)
        for row in rows:
            w.writerow(row)
    return len(rows)


BANK_HEADERS = [
    "Statement Number", "Statement Date", "Account Number", "Account Alias",
    "Account Currency", "Opening Available Balance",
    "Opening Ledger Balance", "Closing Available Balance", "Closing Ledger Balance",
    "Value Date", "Post Date", "Tran Type", "Bank Reference", "Narrative",
    "Debits", "Credits",
]


def write_bank_statement(transfer_rows, random_debits, path: str):
    """Bank statement XLSX: one row per bankTransfer landing in a bank
    account, interleaved by date with the random unmatched Debit rows.

    Header on row 1, no spacer columns. The bank line value is
    -1 * bankTransfer.Amount: usually a Credit, but a Debit when refunds
    dominate a drain window (payments and refunds both settle here).

    cardTransfer rows are NOT included here -- payouts go from the Adyen
    Balance Account directly to the customer's card and don't cross Zurich's
    bank account.

    Random Debit rows have no counterpart in Balance Platform and exercise
    the recon engine's skip-unmatched-row logic.

    Rows from every BANK_META entry land in this single workbook. Each row
    carries its bank's Account Number / Account Alias / Statement Number, and
    running balances are tracked per-bank so each account's ledger remains
    internally consistent.
    """
    combined = []
    for r in transfer_rows:
        combined.append({
            "booking_dt": r["booking_dt"],
            "bank":       r["bank"],
            "bank_amount": -r["amount"],   # bankTransfer.Amount sign-flipped at the bank
            "tran_type":  "TRANSFER",
            "reference":  r["reference_str"],
            "narrative":  f"TRANSFER {r['reference_str']} FROM Adyen Australia ",
        })
    for r in random_debits:
        combined.append({
            "booking_dt": r["booking_dt"],
            "bank":       r["bank"],
            "bank_amount": r["amount"],    # already signed (negative => Debit)
            "tran_type":  r["tran_type"],
            "reference":  r["reference"],
            "narrative":  r["narrative"],
        })
    # Sort by (bank, booking_dt) so rows from each bank are grouped together
    # and contiguous -- a more realistic representation of a multi-account
    # statement extract than rows from two accounts interleaved by date.
    combined.sort(key=lambda r: (r["bank"], r["booking_dt"]))

    running = {bank: bm["opening_balance"] for bank, bm in BANK_META.items()}

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bank Statement"

    # Two preamble rows so the real header lands on row 3 (header_row_index: 2
    # in the recon config). Mirrors how real bank extracts ship with an
    # institution banner / "as-of" line above the column headers.
    preamble_rng = random.Random(SEED ^ 0xB4 if SEED is not None else None)
    extract_id   = f"EXT-{preamble_rng.randint(100000, 999999)}"
    batch_id     = f"BATCH-{preamble_rng.randint(1000, 9999)}"
    ws.append([
        "ZURICH AUSTRALIA - DAILY BANK STATEMENT EXTRACT",
        "", "", "", "", "", "", "",
        f"Extract ID: {extract_id}",
    ])
    ws.append([
        f"As of: {fmt_bank_date(RUN_DATE_END)}",
        "", "", "", "", "", "", "",
        f"Batch: {batch_id}",
    ])

    ws.append(BANK_HEADERS)

    for r in combined:
        bank        = r["bank"]
        bank_meta   = BANK_META[bank]
        bank_amount = r["bank_amount"]
        opening     = running[bank]
        running[bank] = running[bank] + bank_amount
        closing     = running[bank]
        row_date    = r["booking_dt"].date()

        if bank_amount >= 0:
            debit_str  = ""
            credit_str = f"{bank_amount:.2f}"
        else:
            debit_str  = f"{-bank_amount:.2f}"
            credit_str = ""

        ws.append([
            bank_meta["statement_number"], # Statement Number
            fmt_bank_date(row_date),       # Statement Date
            bank_meta["account_number"],   # Account Number
            bank_meta["account_alias"],    # Account Alias
            DEFAULT_CURRENCY,              # Account Currency
            "",                            # Opening Available Balance
            f"{opening:.2f}",              # Opening Ledger Balance
            "",                            # Closing Available Balance
            f"{closing:.2f}",              # Closing Ledger Balance
            fmt_bank_date(row_date),       # Value Date
            fmt_bank_date(row_date),       # Post Date
            r["tran_type"],                # Tran Type
            r["reference"],                # Bank Reference
            r["narrative"],                # Narrative
            debit_str,                     # Debits
            credit_str,                    # Credits
        ])

    wb.save(path)
    return len(combined)


# ---------------------------- Main ----------------------------------------

def main():
    rng = random.Random(SEED)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    txn_counts      = derive_txn_counts()
    n_batches       = derive_n_batches()
    merchant_hours  = derive_merchant_hours()
    run_dates       = derive_run_dates()
    n_adj_per_day   = sum(len(v) for v in merchant_hours.values())
    n_adj_total     = n_adj_per_day * len(run_dates)
    n_drain_total   = sum(
        (len(v) * len(run_dates) + DRAIN_GROUP_SIZE - 1) // DRAIN_GROUP_SIZE
        for v in merchant_hours.values()
    )
    print(f"[config] RUN_NONCE        = {RUN_NONCE}")
    print(f"[config] N_LIFE3_TOTAL    = {N_LIFE3_TOTAL}")
    print(f"[config] date range       = {run_dates[0]} .. {run_dates[-1]} ({len(run_dates)} days)")
    print(f"[config] txn counts       = {txn_counts}")
    print(f"[config] txn type mix     = {TXN_TYPE_MIX}")
    print(f"[config] refund lag days  = [{REFUND_LAG_DAYS_MIN}..{REFUND_LAG_DAYS_MAX}]")
    print(f"[config] n_batches        = {n_batches}")
    print(f"[config] hour partition   = "
          f"{ {m: f'{v[0]:02d}..{v[-1]:02d} ({len(v)}h)' for m, v in merchant_hours.items()} }")
    print(f"[config] expected balance = "
          f"~{n_adj_total} adjustments + ~{n_drain_total} bankTransfers "
          f"(target ratio {BALANCE_TO_LIFE3_RATIO}, drain group {DRAIN_GROUP_SIZE})")
    print(f"[config] mismatch rates   = {MISMATCH}")
    print()

    print("[1/7] Building transactions...")
    txns = build_txns(rng)
    n_payment = sum(1 for t in txns if t.txn_type == "payment")
    n_refund  = sum(1 for t in txns if t.txn_type == "refund")
    n_payout  = sum(1 for t in txns if t.txn_type == "payout")
    print(f"        payments={n_payment}, refunds={n_refund}, payouts={n_payout}")

    print("[2/7] Writing Life3...")
    p_life3 = os.path.join(OUTPUT_DIR, "life3_payments.EXT")
    n_life3 = write_life3(txns, p_life3)

    print("[3/7] Writing GDPP payments...")
    p_gpay = os.path.join(OUTPUT_DIR, "gdpp_payments.csv")
    n_gpay = write_gdpp_payments(txns, p_gpay)

    print("[4/7] Writing GDPP refunds...")
    p_gref = os.path.join(OUTPUT_DIR, "gdpp_refunds.csv")
    n_gref = write_gdpp_refunds(txns, p_gref)

    print("[5/7] Writing GDPP payouts...")
    p_gpou = os.path.join(OUTPUT_DIR, "gdpp_payouts.csv")
    n_gpou = write_gdpp_payouts(txns, p_gpou)

    print("[6/7] Writing Adyen Settlement Detail...")
    p_adyen = os.path.join(OUTPUT_DIR, "adyen_settlement_detail_report.csv")
    n_adyen = write_adyen_settlement(txns, p_adyen)

    print("[7a/7] Aggregating + writing Balance Platform...")
    adj_rows  = aggregate_to_balance(txns, rng)
    ct_rows   = make_card_transfers(txns, rng)
    bt_rows   = make_bank_transfers(adj_rows)
    p_balance = os.path.join(OUTPUT_DIR, "balanceplatform_statement_report.csv")
    n_balance = write_balance_platform(adj_rows, bt_rows, ct_rows, p_balance)

    print("[7b/7] Writing Bank statement...")
    p_bank = os.path.join(OUTPUT_DIR, "bankfile.xlsx")
    random_debits = make_random_bank_debits(rng)
    n_bank = write_bank_statement(bt_rows, random_debits, p_bank)

    print()
    print("=" * 60)
    print(f"  Life3 rows                : {n_life3}")
    print(f"  GDPP payment rows         : {n_gpay}")
    print(f"  GDPP refund rows          : {n_gref}")
    print(f"  GDPP payout rows          : {n_gpou}")
    earliest_adyen_dt = min((t.creation_dt for t in txns if t.in_adyen), default=None)
    earliest_str      = fmt_dt_full(earliest_adyen_dt.replace(microsecond=0)) if earliest_adyen_dt else "n/a"
    print(f"  Adyen Settlement rows     : {n_adyen}")
    print(f"  Earliest Creation Date    : {earliest_str}")
    n_ct_fail = sum(1 for r in ct_rows if r["status"] == "fail")
    print(f"  Balance Platform rows     : {n_balance} "
          f"({len(adj_rows)} adj + {len(bt_rows)} bankTransfer + {len(ct_rows)} cardTransfer"
          f", of which {n_ct_fail} are failed-payout reversals)")
    print(f"  Bank statement rows       : {n_bank} "
          f"({len(bt_rows)} bankTransfer + {len(random_debits)} random unmatched debits)")
    print("=" * 60)
    print(f"Output dir: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
