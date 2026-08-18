import copy
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation


# ============================================================
# QUALITY STATES
# ============================================================

QUALITY_VALID = "valid"
QUALITY_CORRECTED = "corrected"
QUALITY_QUARANTINED = "quarantined"


# ============================================================
# OFFICIAL ASSIGNMENT QUARANTINE CODES
# ============================================================

ERR_ID_ORDER_MISSING = "MISSING_ORDER_ID"
ERR_ID_CUSTOMER_MISSING = "MISSING_CUSTOMER_ID"
ERR_DATE_IMPOSSIBLE_INVALID = "INVALID_IMPOSSIBLE_DATE"
ERR_JSON_ITEMS_CORRUPTED = "CORRUPTED_ITEMS_JSON"
ERR_ITEMS_EMPTY = "EMPTY_ITEMS"
ERR_PRICE_UNKNOWN = "UNKNOWN_PRICE"
ERR_VALUE_NEGATIVE_AMBIGUOUS = "AMBIGUOUS_NEGATIVE_VALUE"
ERR_ID_ORDER_DUPLICATE = "DUPLICATE_ORDER_ID"
ERR_ERRORS_CONFLICTING_MULTIPLE = "MULTIPLE_CONFLICTING_ERRORS"


# Extra explicit codes for real dataset cases.
ERR_EMAIL_INVALID = "EMAIL_INVALID_UNRECOVERABLE"
ERR_PHONE_INVALID = "PHONE_INVALID_UNRECOVERABLE"
ERR_CURRENCY_UNKNOWN = "CURRENCY_UNKNOWN"
ERR_STATUS_UNKNOWN = "STATUS_UNKNOWN"
ERR_ITEM_QUANTITY_INVALID = "ITEM_QUANTITY_INVALID"
ERR_ITEM_COMPONENTS_CONFLICT = "ITEM_COMPONENTS_CONFLICT"
ERR_TOTAL_UNKNOWN = "TOTAL_UNKNOWN_UNRECOVERABLE"


# ============================================================
# RULE CODES
# ============================================================

RULE_WHITESPACE = "WHITESPACE_TRIMMED"
RULE_ARABIC_DIGITS = "ARABIC_DIGITS_TO_LATIN"
RULE_ARABIC_DECIMAL = "ARABIC_DECIMAL_SEPARATOR_NORMALIZED"
RULE_THOUSANDS = "THOUSANDS_SEPARATOR_NORMALIZED"
RULE_KNOWN_PRICE_WORD = "KNOWN_PRICE_WORD_TO_NUMBER"
RULE_CURRENCY_SUFFIX = "CURRENCY_SUFFIX_REMOVED"
RULE_CURRENCY_YER = "CURRENCY_NORMALIZED_YER"
RULE_PHONE = "PHONE_NORMALIZED_YE"
RULE_EMAIL = "EMAIL_REPEATED_SYMBOLS"
RULE_DATE = "DATE_NORMALIZED"
RULE_STATUS_SYNONYM = "STATUS_SYNONYM_NORMALIZED"
RULE_NEGATIVE_QTY = "NEGATIVE_QTY_DERIVED"
RULE_ITEM_TOTAL = "ITEM_TOTAL_DERIVED"
RULE_ITEM_PRICE_DIRECT = "ITEM_PRICE_DIRECT_DERIVED"
RULE_ITEM_TOTAL_RESIDUAL = "ITEM_TOTAL_RESIDUAL_DERIVED"
RULE_ITEM_PRICE_RESIDUAL = "ITEM_PRICE_RESIDUAL_DERIVED"
RULE_ORDER_TOTAL_DERIVED = "ORDER_TOTAL_DERIVED"
RULE_ORDER_TOTAL_RECALCULATED = "ORDER_TOTAL_RECALCULATED"


# ============================================================
# CONSTANTS
# ============================================================

ARABIC_DIGITS_RE = re.compile(r"[٠-٩۰-۹]")

ARABIC_DIGITS_MAP = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
    "01234567890123456789"
)

KNOWN_PRICE_WORDS = {
    "ألفان": Decimal("2000"),
    "خمسة آلاف": Decimal("5000"),
}

VALID_ORDER_STATUSES = {
    "مؤكد",
    "قيد الانتظار",
    "مرتجع",
    "قيد الشحن",
    "تم التسليم",
    "ملغي",
}

# Explicit deterministic synonym dictionary.
PAYMENT_STATUS_MAP = {
    "تم الدفع": "تم الدفع",
    "بانتظار الدفع": "بانتظار الدفع",
    "مدفوع": "تم الدفع",
}

REPEATED_AT = re.compile(r"@{2,}")
REPEATED_DOT = re.compile(r"\.{2,}")


# ============================================================
# BASIC HELPERS
# ============================================================

def is_blank(value):
    return value is None or str(value).strip() == ""


def normalize_digits(value):
    return str(value).translate(ARABIC_DIGITS_MAP)


def to_number(value):
    if value is None:
        return None

    if value == value.to_integral_value():
        return int(value)

    return float(value)


def positive_integer(value):
    return (
        value is not None
        and value > 0
        and value == value.to_integral_value()
    )


def parse_decimal(value):
    """
    Deterministic numeric parser.

    Supports only transformations justified by the assignment
    and confirmed in the real dataset.
    """

    if is_blank(value):
        return None

    text = str(value).strip()

    if text in KNOWN_PRICE_WORDS:
        return KNOWN_PRICE_WORDS[text]

    text = normalize_digits(text)

    # Arabic separators.
    text = text.replace("٫", ".")
    text = text.replace("٬", ",")

    # Remove explicit known currency suffixes only.
    text = re.sub(
        r"\s*(YER|ريال يمني|ريال)\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    )

    # Thousands separators.
    text = text.strip().replace(",", "")

    try:
        return Decimal(text)

    except InvalidOperation:
        return None


# ============================================================
# AUDIT HELPERS
# ============================================================

def add_correction(
    corrections,
    field,
    original,
    corrected,
    rule_code,
    details=None,
):
    correction = {
        "field": field,
        "original_value": original,
        "corrected_value": corrected,
        "rule_code": rule_code,
    }

    if details:
        correction["details"] = details

    corrections.append(correction)


def add_error(
    errors,
    code,
    field,
    value,
    message,
):
    errors.append(
        {
            "code": code,
            "field": field,
            "value": value,
            "message": message,
        }
    )


# ============================================================
# NUMERIC FORMAT AUDIT
# ============================================================

def numeric_rule_codes(original):
    if is_blank(original):
        return []

    text = str(original).strip()

    codes = []

    if text in KNOWN_PRICE_WORDS:
        codes.append(RULE_KNOWN_PRICE_WORD)

    if ARABIC_DIGITS_RE.search(text):
        codes.append(RULE_ARABIC_DIGITS)

    if "٫" in text:
        codes.append(RULE_ARABIC_DECIMAL)

    if "," in text or "٬" in text:
        codes.append(RULE_THOUSANDS)

    if re.search(
        r"(ريال يمني|ريال)\s*$",
        text
    ):
        codes.append(RULE_CURRENCY_SUFFIX)

    return codes


def normalize_numeric_field(
    cleaned,
    field,
    corrections,
):
    original = cleaned.get(field)

    parsed = parse_decimal(original)

    if parsed is None:
        return None

    corrected = to_number(parsed)

    cleaned[field] = corrected

    for rule_code in numeric_rule_codes(original):

        add_correction(
            corrections,
            field,
            original,
            corrected,
            rule_code,
        )

    return parsed


# ============================================================
# WHITESPACE
# ============================================================

def normalize_top_level_whitespace(
    cleaned,
    corrections,
):
    for field, value in list(cleaned.items()):

        if not isinstance(value, str):
            continue

        trimmed = value.strip()

        if trimmed != value:

            add_correction(
                corrections,
                field,
                value,
                trimmed,
                RULE_WHITESPACE,
            )

            cleaned[field] = trimmed


# ============================================================
# DATE
# ============================================================

def normalize_date(
    cleaned,
    corrections,
    errors,
):
    original = cleaned.get("order_date")

    if is_blank(original):

        add_error(
            errors,
            ERR_DATE_IMPOSSIBLE_INVALID,
            "order_date",
            original,
            "Order date is missing.",
        )

        return

    text = normalize_digits(
        str(original).strip()
    )

    # Already valid ISO.
    try:
        parsed = datetime.fromisoformat(
            text.replace("Z", "+00:00")
        )

        canonical = parsed.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        cleaned["order_date"] = canonical

        return

    except ValueError:
        pass

    known_formats = [
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]

    for fmt in known_formats:

        try:
            parsed = datetime.strptime(
                text,
                fmt
            )

            canonical = parsed.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

            cleaned["order_date"] = canonical

            add_correction(
                corrections,
                "order_date",
                original,
                canonical,
                RULE_DATE,
            )

            return

        except ValueError:
            continue

    add_error(
        errors,
        ERR_DATE_IMPOSSIBLE_INVALID,
        "order_date",
        original,
        "Date is impossible or cannot be converted by a known safe format.",
    )


# ============================================================
# EMAIL
# ============================================================

def email_is_valid(value):
    if is_blank(value):
        return False

    text = str(value).strip()

    if " " in text:
        return False

    if text.count("@") != 1:
        return False

    if ".." in text:
        return False

    local, domain = text.split("@", 1)

    if not local or not domain:
        return False

    if (
        local.startswith(".")
        or local.endswith(".")
        or domain.startswith(".")
        or domain.endswith(".")
    ):
        return False

    if "." not in domain:
        return False

    if any(
        not label
        for label in domain.split(".")
    ):
        return False

    return True


def normalize_email(
    cleaned,
    corrections,
    errors,
):
    original = cleaned.get(
        "customer_email"
    )

    if is_blank(original):

        add_error(
            errors,
            ERR_EMAIL_INVALID,
            "customer_email",
            original,
            "Email is missing.",
        )

        return

    text = str(original).strip()

    if email_is_valid(text):
        cleaned["customer_email"] = text
        return

    candidate = REPEATED_AT.sub(
        "@",
        text
    )

    candidate = REPEATED_DOT.sub(
        ".",
        candidate
    )

    if (
        candidate != text
        and email_is_valid(candidate)
    ):

        cleaned["customer_email"] = (
            candidate
        )

        add_correction(
            corrections,
            "customer_email",
            original,
            candidate,
            RULE_EMAIL,
        )

        return

    add_error(
        errors,
        ERR_EMAIL_INVALID,
        "customer_email",
        original,
        "Email cannot be repaired deterministically.",
    )


# ============================================================
# PHONE
# ============================================================

def normalize_phone(
    cleaned,
    corrections,
    errors,
):
    original = cleaned.get(
        "customer_phone"
    )

    if is_blank(original):

        add_error(
            errors,
            ERR_PHONE_INVALID,
            "customer_phone",
            original,
            "Phone number is missing.",
        )

        return

    text = normalize_digits(
        str(original).strip()
    )

    compact = re.sub(
        r"[\s\-()]",
        "",
        text
    )

    corrected = None

    # +967 + 9 local digits
    if compact.startswith("+967"):

        national = compact[4:]

        if (
            national.isdigit()
            and len(national) == 9
        ):
            corrected = national

    # Already canonical local format.
    elif (
        compact.isdigit()
        and len(compact) == 9
    ):
        corrected = compact

    if corrected is None:

        add_error(
            errors,
            ERR_PHONE_INVALID,
            "customer_phone",
            original,
            "Phone cannot be safely normalized to the 9-digit Yemeni format.",
        )

        return

    cleaned["customer_phone"] = (
        corrected
    )

    if str(original).strip() != corrected:

        add_correction(
            corrections,
            "customer_phone",
            original,
            corrected,
            RULE_PHONE,
        )


# ============================================================
# CURRENCY
# ============================================================

def normalize_currency(
    cleaned,
    corrections,
    errors,
):
    original = cleaned.get("currency")

    if is_blank(original):

        add_error(
            errors,
            ERR_CURRENCY_UNKNOWN,
            "currency",
            original,
            "Currency is missing.",
        )

        return

    text = str(original).strip()

    if text.upper() == "YER":

        cleaned["currency"] = "YER"

        if text != "YER":

            add_correction(
                corrections,
                "currency",
                original,
                "YER",
                RULE_CURRENCY_YER,
            )

        return

    if text in {
        "ريال يمني",
        "ريال",
    }:

        cleaned["currency"] = "YER"

        add_correction(
            corrections,
            "currency",
            original,
            "YER",
            RULE_CURRENCY_YER,
        )

        return

    add_error(
        errors,
        ERR_CURRENCY_UNKNOWN,
        "currency",
        original,
        "Currency is unknown; assigning YER would be guessing.",
    )


# ============================================================
# STATUS
# ============================================================

def normalize_statuses(
    cleaned,
    corrections,
    errors,
):
    status = cleaned.get("status")

    if (
        is_blank(status)
        or str(status).strip()
        not in VALID_ORDER_STATUSES
    ):

        add_error(
            errors,
            ERR_STATUS_UNKNOWN,
            "status",
            status,
            "Order status is not in the known canonical status set.",
        )

    else:

        cleaned["status"] = (
            str(status).strip()
        )

    payment_status = cleaned.get(
        "payment_status"
    )

    if not is_blank(payment_status):

        text = str(payment_status).strip()

        canonical = PAYMENT_STATUS_MAP.get(
            text
        )

        if canonical is not None:

            cleaned["payment_status"] = (
                canonical
            )

            if canonical != text:

                add_correction(
                    corrections,
                    "payment_status",
                    payment_status,
                    canonical,
                    RULE_STATUS_SYNONYM,
                )


# ============================================================
# ITEMS JSON
# ============================================================

def parse_items(
    cleaned,
    errors,
):
    value = cleaned.get("items_json")

    if is_blank(value):

        add_error(
            errors,
            ERR_ITEMS_EMPTY,
            "items_json",
            value,
            "Order has no items.",
        )

        return None

    try:
        items = json.loads(value)

    except (
        json.JSONDecodeError,
        TypeError,
    ):

        add_error(
            errors,
            ERR_JSON_ITEMS_CORRUPTED,
            "items_json",
            value,
            "items_json cannot be parsed.",
        )

        return None

    if not isinstance(items, list):

        add_error(
            errors,
            ERR_JSON_ITEMS_CORRUPTED,
            "items_json",
            value,
            "items_json must be a list.",
        )

        return None

    if len(items) == 0:

        add_error(
            errors,
            ERR_ITEMS_EMPTY,
            "items_json",
            value,
            "Items list is empty.",
        )

        return None

    if any(
        not isinstance(item, dict)
        for item in items
    ):

        add_error(
            errors,
            ERR_JSON_ITEMS_CORRUPTED,
            "items_json",
            value,
            "One or more items are not objects.",
        )

        return None

    return [
        dict(item)
        for item in items
    ]


def normalize_item_whitespace(
    items,
    corrections,
):
    for index, item in enumerate(items):

        for key, value in list(
            item.items()
        ):

            if not isinstance(value, str):
                continue

            trimmed = value.strip()

            if trimmed != value:

                item[key] = trimmed

                add_correction(
                    corrections,
                    f"items_json[{index}].{key}",
                    value,
                    trimmed,
                    RULE_WHITESPACE,
                )


def item_number(
    item,
    field,
    index,
    corrections,
):
    original = item.get(field)

    parsed = parse_decimal(original)

    if parsed is None:
        return None

    corrected = to_number(parsed)

    item[field] = corrected

    for rule_code in numeric_rule_codes(
        original
    ):

        add_correction(
            corrections,
            f"items_json[{index}].{field}",
            original,
            corrected,
            rule_code,
        )

    return parsed


def order_total_corroborates_items(
    items,
    delivery,
    order_total,
):
    if (
        delivery is None
        or delivery < 0
        or order_total is None
        or order_total < 0
    ):
        return False

    totals = []

    for item in items:

        value = parse_decimal(
            item.get("total")
        )

        if (
            value is None
            or value < 0
        ):
            return False

        totals.append(value)

    return (
        sum(
            totals,
            Decimal("0")
        )
        + delivery
        == order_total
    )


def clean_items(
    cleaned,
    corrections,
    errors,
    delivery,
    order_total,
    payment_amount,
):
    items = parse_items(
        cleaned,
        errors
    )

    if items is None:
        return None

    normalize_item_whitespace(
        items,
        corrections
    )

    # Independent evidence used for negative qty recovery.
    order_corroborated = (
        order_total_corroborates_items(
            items,
            delivery,
            order_total,
        )
    )

    states = []

    # --------------------------------------------------------
    # FIRST PASS
    # --------------------------------------------------------

    for index, item in enumerate(items):

        qty = item_number(
            item,
            "qty",
            index,
            corrections,
        )

        unit_price = item_number(
            item,
            "unit_price",
            index,
            corrections,
        )

        item_total = item_number(
            item,
            "total",
            index,
            corrections,
        )

        # Negative quantity.
        if (
            qty is not None
            and qty < 0
        ):

            candidate = None

            if (
                unit_price is not None
                and unit_price > 0
                and item_total is not None
                and item_total >= 0
            ):

                candidate = (
                    item_total
                    / unit_price
                )

            if (
                positive_integer(candidate)
                and order_corroborated
            ):

                original_qty = item["qty"]

                qty = candidate

                item["qty"] = to_number(
                    candidate
                )

                add_correction(
                    corrections,
                    f"items_json[{index}].qty",
                    original_qty,
                    item["qty"],
                    RULE_NEGATIVE_QTY,
                    details=(
                        "Derived as item_total / unit_price "
                        "and corroborated by order total."
                    ),
                )

            else:

                add_error(
                    errors,
                    ERR_VALUE_NEGATIVE_AMBIGUOUS,
                    f"items_json[{index}].qty",
                    item.get("qty"),
                    "Negative quantity cannot be resolved safely.",
                )

        if (
            qty is None
            or qty == 0
        ):

            add_error(
                errors,
                ERR_ITEM_QUANTITY_INVALID,
                f"items_json[{index}].qty",
                item.get("qty"),
                "Quantity is missing, non-numeric, or zero.",
            )

        if (
            unit_price is not None
            and unit_price < 0
        ):

            add_error(
                errors,
                ERR_VALUE_NEGATIVE_AMBIGUOUS,
                f"items_json[{index}].unit_price",
                item.get("unit_price"),
                "Negative unit price is ambiguous.",
            )

        if (
            item_total is not None
            and item_total < 0
        ):

            add_error(
                errors,
                ERR_VALUE_NEGATIVE_AMBIGUOUS,
                f"items_json[{index}].total",
                item.get("total"),
                "Negative item total is ambiguous.",
            )

        states.append(
            {
                "index": index,
                "item": item,
                "qty": qty,
                "unit_price": unit_price,
                "item_total": item_total,
            }
        )

    # --------------------------------------------------------
    # DIRECT SAFE DERIVATIONS
    # --------------------------------------------------------

    for state in states:

        index = state["index"]
        item = state["item"]

        qty = state["qty"]
        price = state["unit_price"]
        total = state["item_total"]

        # total = qty * price
        if (
            total is None
            and positive_integer(qty)
            and price is not None
            and price >= 0
        ):

            derived = qty * price

            original = item.get("total")

            item["total"] = to_number(
                derived
            )

            state["item_total"] = (
                derived
            )

            add_correction(
                corrections,
                f"items_json[{index}].total",
                original,
                item["total"],
                RULE_ITEM_TOTAL,
                details="Derived as qty * unit_price.",
            )

        # price = total / qty
        if (
            state["unit_price"] is None
            and positive_integer(qty)
            and state["item_total"]
            is not None
            and state["item_total"] >= 0
        ):

            derived = (
                state["item_total"]
                / qty
            )

            original = item.get(
                "unit_price"
            )

            item["unit_price"] = (
                to_number(derived)
            )

            state["unit_price"] = (
                derived
            )

            add_correction(
                corrections,
                f"items_json[{index}].unit_price",
                original,
                item["unit_price"],
                RULE_ITEM_PRICE_DIRECT,
                details="Derived as item_total / qty.",
            )

    # --------------------------------------------------------
    # RESIDUAL RECOVERY
    # --------------------------------------------------------

    targets = [
        state
        for state in states
        if (
            state["unit_price"] is None
            and state["item_total"] is None
            and positive_integer(
                state["qty"]
            )
        )
    ]

    if len(targets) == 1:

        target = targets[0]

        others = [
            state
            for state in states
            if state is not target
        ]

        if (
            order_total is not None
            and order_total >= 0
            and delivery is not None
            and delivery >= 0
            and payment_amount is not None
            and payment_amount == order_total
            and all(
                state["item_total"] is not None
                and state["item_total"] >= 0
                for state in others
            )
        ):

            residual = (
                order_total
                - delivery
                - sum(
                    (
                        state["item_total"]
                        for state in others
                    ),
                    Decimal("0"),
                )
            )

            if residual >= 0:

                item = target["item"]
                index = target["index"]
                qty = target["qty"]

                original_total = (
                    item.get("total")
                )

                original_price = (
                    item.get("unit_price")
                )

                target["item_total"] = (
                    residual
                )

                target["unit_price"] = (
                    residual / qty
                )

                item["total"] = to_number(
                    target["item_total"]
                )

                item["unit_price"] = (
                    to_number(
                        target["unit_price"]
                    )
                )

                add_correction(
                    corrections,
                    f"items_json[{index}].total",
                    original_total,
                    item["total"],
                    RULE_ITEM_TOTAL_RESIDUAL,
                    details=(
                        "Residual = order_total - delivery "
                        "- other item totals."
                    ),
                )

                add_correction(
                    corrections,
                    f"items_json[{index}].unit_price",
                    original_price,
                    item["unit_price"],
                    RULE_ITEM_PRICE_RESIDUAL,
                    details=(
                        "Derived from residual item total / qty; "
                        "payment_amount corroborates order_total."
                    ),
                )

    # --------------------------------------------------------
    # FINAL ITEM VALIDATION
    # --------------------------------------------------------

    for state in states:

        index = state["index"]

        qty = state["qty"]
        price = state["unit_price"]
        total = state["item_total"]

        if (
            price is None
            or total is None
        ):

            add_error(
                errors,
                ERR_PRICE_UNKNOWN,
                f"items_json[{index}]",
                state["item"],
                (
                    "Price or item total remains unknown after "
                    "all deterministic derivations."
                ),
            )

            continue

        # If a conflict remains after safe corrections,
        # do not guess which component is wrong.
        if (
            qty is not None
            and qty > 0
            and price >= 0
            and total >= 0
            and qty * price != total
        ):

            add_error(
                errors,
                ERR_ITEM_COMPONENTS_CONFLICT,
                f"items_json[{index}]",
                state["item"],
                (
                    "qty, unit_price and total remain inconsistent "
                    "after safe corrections."
                ),
            )

    cleaned["items_json"] = json.dumps(
        items,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return states


# ============================================================
# MAIN RECORD CLASSIFIER
# ============================================================

def classify_record(
    raw_record,
    duplicate_conflict=False,
):
    cleaned = copy.deepcopy(
        raw_record
    )

    corrections = []
    errors = []

    # General trim first.
    normalize_top_level_whitespace(
        cleaned,
        corrections
    )

    # --------------------------------------------------------
    # REQUIRED IDS
    # --------------------------------------------------------

    if is_blank(
        cleaned.get("order_id")
    ):

        add_error(
            errors,
            ERR_ID_ORDER_MISSING,
            "order_id",
            cleaned.get("order_id"),
            "Order ID is missing and cannot be inferred.",
        )

    if is_blank(
        cleaned.get("customer_id")
    ):

        add_error(
            errors,
            ERR_ID_CUSTOMER_MISSING,
            "customer_id",
            cleaned.get("customer_id"),
            "Customer ID is missing and cannot be inferred.",
        )

    if duplicate_conflict:

        add_error(
            errors,
            ERR_ID_ORDER_DUPLICATE,
            "order_id",
            cleaned.get("order_id"),
            (
                "order_id belongs to a genuinely conflicting "
                "duplicate group."
            ),
        )

    # --------------------------------------------------------
    # TEXT / FORMAT RULES
    # --------------------------------------------------------

    normalize_date(
        cleaned,
        corrections,
        errors
    )

    normalize_email(
        cleaned,
        corrections,
        errors
    )

    normalize_phone(
        cleaned,
        corrections,
        errors
    )

    normalize_currency(
        cleaned,
        corrections,
        errors
    )

    normalize_statuses(
        cleaned,
        corrections,
        errors
    )

    # --------------------------------------------------------
    # TOP-LEVEL NUMBERS
    # --------------------------------------------------------

    delivery = normalize_numeric_field(
        cleaned,
        "delivery_cost",
        corrections,
    )

    payment_amount = normalize_numeric_field(
        cleaned,
        "payment_amount",
        corrections,
    )

    order_total = normalize_numeric_field(
        cleaned,
        "total_amount",
        corrections,
    )

    # --------------------------------------------------------
    # ITEMS
    # --------------------------------------------------------

    states = clean_items(
        cleaned,
        corrections,
        errors,
        delivery,
        order_total,
        payment_amount,
    )

    # --------------------------------------------------------
    # ORDER TOTAL
    # --------------------------------------------------------

    if (
        states is not None
        and delivery is not None
        and delivery >= 0
    ):

        all_totals_known = all(
            state["item_total"]
            is not None
            and state["item_total"] >= 0
            for state in states
        )

        if all_totals_known:

            expected_total = (
                sum(
                    (
                        state["item_total"]
                        for state in states
                    ),
                    Decimal("0"),
                )
                + delivery
            )

            if order_total is None:

                original = raw_record.get(
                    "total_amount"
                )

                cleaned["total_amount"] = (
                    to_number(
                        expected_total
                    )
                )

                order_total = expected_total

                add_correction(
                    corrections,
                    "total_amount",
                    original,
                    cleaned["total_amount"],
                    RULE_ORDER_TOTAL_DERIVED,
                    details=(
                        "Derived as sum(item totals) "
                        "+ delivery_cost."
                    ),
                )

            elif order_total != expected_total:

                original = cleaned.get(
                    "total_amount"
                )

                cleaned["total_amount"] = (
                    to_number(
                        expected_total
                    )
                )

                order_total = expected_total

                add_correction(
                    corrections,
                    "total_amount",
                    original,
                    cleaned["total_amount"],
                    RULE_ORDER_TOTAL_RECALCULATED,
                    details=(
                        "Recalculated as sum(item totals) "
                        "+ delivery_cost."
                    ),
                )

        elif order_total is None:

            add_error(
                errors,
                ERR_TOTAL_UNKNOWN,
                "total_amount",
                raw_record.get(
                    "total_amount"
                ),
                (
                    "Order total is unknown and cannot "
                    "be safely recomputed."
                ),
            )

    elif order_total is None:

        add_error(
            errors,
            ERR_TOTAL_UNKNOWN,
            "total_amount",
            raw_record.get(
                "total_amount"
            ),
            (
                "Order total is unknown and required "
                "components are unusable."
            ),
        )

    # --------------------------------------------------------
    # MULTIPLE CONFLICTING ERRORS
    # --------------------------------------------------------

    codes_error = []

    for error in errors:

        code = error["code"]

        if code not in codes_error:
            codes_error.append(code)

    if (
        len(codes_error) > 1
        and ERR_ERRORS_CONFLICTING_MULTIPLE
        not in codes_error
    ):

        add_error(
            errors,
            ERR_ERRORS_CONFLICTING_MULTIPLE,
            "_record",
            None,
            (
                "Multiple substantial errors prevent "
                "a single safe correction path."
            ),
        )

        codes_error.append(
            ERR_ERRORS_CONFLICTING_MULTIPLE
        )

    # --------------------------------------------------------
    # FINAL CLASSIFICATION
    # --------------------------------------------------------

    if errors:

        quality_status = (
            QUALITY_QUARANTINED
        )

    elif corrections:

        quality_status = (
            QUALITY_CORRECTED
        )

    else:

        quality_status = (
            QUALITY_VALID
        )

    return {
        "quality_status": quality_status,
        "cleaned_record": cleaned,
        "corrections": corrections,
        "codes_error": codes_error,
        "details_error": errors,
    }
