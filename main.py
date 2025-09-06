import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Tuple

from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from tinkoff.invest import (
    Client,
    InstrumentIdType,
    OperationState,
    OperationType,
    OrderDirection,
    OrderType,
    RequestError,
    TradesStreamResponse,
    PortfolioResponse,
)

# ===================== ENV =====================
load_dotenv()

MASTER_TOKEN = os.getenv("MASTER_TOKEN", "")
SLAVE_TOKEN = os.getenv("SLAVE_TOKEN", "")
MASTER = os.getenv("MASTER_ACCOUNT_ID", "")
SLAVE = os.getenv("SLAVE_ACCOUNT_ID", "")

COEFF = float(os.getenv("COEFF", "1.0"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "10"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
ALLOW_FIGI = [x.strip() for x in os.getenv(
    "ALLOW_FIGI", "").split(",") if x.strip()]
DENY_FIGI = [x.strip() for x in os.getenv(
    "DENY_FIGI", "").split(",") if x.strip()]
MIN_LOTS = int(os.getenv("MIN_LOTS", "1"))
MAX_LOTS_PER_ORDER = int(os.getenv("MAX_LOTS_PER_ORDER", "1000"))
STATE_FILE = Path(os.getenv("STATE_FILE", "mirror_state.json"))

assert (
    MASTER_TOKEN and SLAVE_TOKEN and MASTER and SLAVE
), "Заполните .env: MASTER_TOKEN, SLAVE_TOKEN, MASTER_ACCOUNT_ID, SLAVE_ACCOUNT_ID"


# ===================== STATE =====================
class State:
    def __init__(self, path: Path):
        self.path = path
        self.data = {"last_from": None, "processed_ids": []}
        if path.exists():
            try:
                self.data = json.loads(path.read_text())
            except Exception:
                pass

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=2))
        tmp.replace(self.path)

    @property
    def last_from(self) -> datetime:
        val = self.data.get("last_from")
        if val:
            return datetime.fromisoformat(val)
        return datetime.now(timezone.utc) - relativedelta(minutes=15)

    @last_from.setter
    def last_from(self, dt: datetime):
        self.data["last_from"] = dt.astimezone(timezone.utc).isoformat()

    def seen(self, op_id: str) -> bool:
        return op_id in self.data.get("processed_ids", [])

    def mark_seen(self, op_id: str, keep_last_n: int = 10000):
        ids = self.data.setdefault("processed_ids", [])
        ids.append(op_id)
        if len(ids) > keep_last_n:
            del ids[: len(ids) - keep_last_n]


STATE = State(STATE_FILE)

# ===================== HELPERS =====================
# Фильтруем «валютные» позиции (их не копируем): RUB/USD/EUR том/туд
CURRENCY_FIGI_SET = {
    "RUB000UTSTOM",
    "USD000UTSTOM",
    "EUR_RUB__TOM",
    "USD000UTSTOD",
    "EUR_RUB__TOD",  # на всякий случай
}


def is_currency_figi(figi: str) -> bool:
    return figi in CURRENCY_FIGI_SET or figi.startswith(("RUB", "USD", "EUR"))


def is_allowed(figi: str) -> bool:
    if is_currency_figi(figi):
        return False
    if ALLOW_FIGI and figi not in ALLOW_FIGI:
        return False
    if DENY_FIGI and figi in DENY_FIGI:
        return False
    return True


def map_direction_from_operation(op_type: OperationType):
    if op_type == OperationType.OPERATION_TYPE_BUY:
        return OrderDirection.ORDER_DIRECTION_BUY
    if op_type == OperationType.OPERATION_TYPE_SELL:
        return OrderDirection.ORDER_DIRECTION_SELL
    return None


def clamp_lots(lots: int) -> int:
    lots = max(MIN_LOTS, lots)
    lots = min(MAX_LOTS_PER_ORDER, lots)
    return lots


def get_instrument_meta(client: Client, figi: str) -> Tuple[int, str]:
    ins = client.instruments.get_instrument_by(
        id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
        id=figi,
    ).instrument
    lot = ins.lot or 1
    ticker = ins.ticker or figi
    return lot, ticker


def place_market_order(
    slave_client: Client,
    account_id: str,
    figi: str,
    direction: OrderDirection,
    lots: int,
):
    order_id = str(uuid.uuid4())
    print(
        f"  -> SLAVE {direction.name} {lots} lot(s) {figi} (order_id={order_id})")
    if DRY_RUN:
        return True
    try:
        slave_client.orders.post_order(
            account_id=account_id,
            instrument_id=figi,  # FIGI допустим как instrument_id
            order_id=order_id,
            quantity=lots,
            direction=direction,
            order_type=OrderType.ORDER_TYPE_MARKET,
        )
        return True
    except RequestError as re:
        print(f"  !! order rejected: {re}")
        return False
    except Exception as e:
        print(f"  !! order error: {e}")
        return False


# ===================== PORTFOLIO / REBALANCE =====================
def read_portfolio_positions(client: Client, account_id: str) -> Dict[str, int]:
    lots_by_figi: Dict[str, int] = {}
    try:
        p: PortfolioResponse = client.operations.get_portfolio(
            account_id=account_id)
        for pos in p.positions:
            figi = pos.figi
            if not figi or is_currency_figi(figi):
                continue
            lots = int(getattr(pos.quantity, "units", 0))
            if lots != 0:
                lots_by_figi[figi] = lots
    except Exception as e:
        print(f"[WARN] read_portfolio_positions({account_id}) failed: {e}")
    return lots_by_figi


_last_rebalance_ts = 0.0
REBALANCE_COOLDOWN_SEC = 120.0


def rebalance_slave_to_master(master_client: Client, slave_client: Client):
    print("[REBALANCE] start")
    master_pos = read_portfolio_positions(master_client, MASTER)
    slave_pos = read_portfolio_positions(slave_client, SLAVE)

    for figi, master_lots in master_pos.items():
        if not is_allowed(figi):
            continue
        target_slave_lots = int(round(master_lots * COEFF))
        current_slave_lots = slave_pos.get(figi, 0)
        delta = target_slave_lots - current_slave_lots
        if delta == 0:
            continue
        lot, ticker = get_instrument_meta(
            master_client, figi
        )  # meta из любого клиента ок
        if delta > 0:
            lots = clamp_lots(delta)
            print(
                f"[REB] {ticker} {figi}: BUY {lots} (target {target_slave_lots}, have {
                    current_slave_lots})"
            )
            place_market_order(
                slave_client, SLAVE, figi, OrderDirection.ORDER_DIRECTION_BUY, lots
            )
        else:
            lots = clamp_lots(-delta)
            print(
                f"[REB] {ticker} {figi}: SELL {lots} (target {target_slave_lots}, have {
                    current_slave_lots})"
            )
            place_market_order(
                slave_client, SLAVE, figi, OrderDirection.ORDER_DIRECTION_SELL, lots
            )
    print("[REBALANCE] done")


def rebalance_with_cooldown(master_client: Client, slave_client: Client):
    global _last_rebalance_ts
    now = time.time()
    if now - _last_rebalance_ts < REBALANCE_COOLDOWN_SEC:
        print("[REBALANCE] skipped (cooldown)")
        return
    rebalance_slave_to_master(master_client, slave_client)
    _last_rebalance_ts = now


# ===================== STREAM =====================
async def mirror_via_trades_stream():
    """
    Ловим сделки мастера (OrdersStreamService.trades_stream) и зеркалим на слейв с кэфом.
    Возвращаем False при ошибке, чтобы верхний цикл переключил fallback.
    """
    print("[STREAM] connect… Dry-run:", DRY_RUN)
    try:
        with Client(MASTER_TOKEN) as master_client, Client(SLAVE_TOKEN) as slave_client:
            rebalance_with_cooldown(master_client, slave_client)

            # ВАЖНО: trades_stream принимает именованный аргумент accounts
            stream = master_client.orders_stream.trades_stream(accounts=[
                                                               MASTER])

            for resp in stream:
                if not isinstance(resp, TradesStreamResponse):
                    continue
                ev = resp.order_trades
                if not ev:
                    continue
                figi = ev.figi
                if not figi or not is_allowed(figi):
                    continue

                master_lots = sum(int(t.quantity) for t in ev.trades)
                if master_lots <= 0:
                    continue

                desired_lots = clamp_lots(int(round(master_lots * COEFF)))
                if desired_lots <= 0:
                    continue

                lot, ticker = get_instrument_meta(master_client, figi)
                print(
                    f"[STREAM] {ticker} {figi}: {ev.direction.name} master={
                        master_lots} -> slave={desired_lots}"
                )
                place_market_order(
                    slave_client, SLAVE, figi, ev.direction, desired_lots
                )
        return True
    except Exception as e:
        print(f"[STREAM] error: {e}")
        return False


# ===================== POLLING (fallback) =====================
def poll_and_mirror_once(master_client: Client, slave_client: Client):
    from_ts = STATE.last_from
    to_ts = datetime.now(timezone.utc)

    ops = master_client.operations.get_operations(
        account_id=MASTER, from_=from_ts, to=to_ts
    ).operations

    new_ops = []
    for op in ops:
        if op.state != OperationState.OPERATION_STATE_EXECUTED:
            continue
        if op.operation_type not in (
            OperationType.OPERATION_TYPE_BUY,
            OperationType.OPERATION_TYPE_SELL,
        ):
            continue
        if not op.figi or not is_allowed(op.figi):
            continue
        if not op.id or STATE.seen(op.id):
            continue
        lots = int(op.quantity or 0) or int(getattr(op, "quantity_lots", 0))
        if lots <= 0:
            continue
        new_ops.append(op)

    if new_ops:
        print(f"[POLL] {len(new_ops)} new executed ops")

    for op in sorted(new_ops, key=lambda x: x.date):
        direction = map_direction_from_operation(op.operation_type)
        if direction is None:
            STATE.mark_seen(op.id)
            continue

        figi = op.figi
        master_lots = int(op.quantity or 0) or int(
            getattr(op, "quantity_lots", 0))
        desired_lots = clamp_lots(int(round(master_lots * COEFF)))
        if desired_lots <= 0:
            STATE.mark_seen(op.id)
            continue

        try:
            lot, ticker = get_instrument_meta(master_client, figi)
        except Exception as e:
            print(f"[POLL] meta err {figi}: {e}")
            STATE.mark_seen(op.id)
            continue

        print(
            f"[POLL] {ticker} {figi}: {direction.name} {
                master_lots} -> {desired_lots}"
        )
        place_market_order(slave_client, SLAVE, figi, direction, desired_lots)
        STATE.mark_seen(op.id)

    STATE.last_from = to_ts
    STATE.save()


async def run_with_fallback():
    while True:
        ok = await mirror_via_trades_stream()
        if ok:
            continue
        print("[MAIN] switching to polling fallback")
        with Client(MASTER_TOKEN) as master_client, Client(SLAVE_TOKEN) as slave_client:
            rebalance_with_cooldown(master_client, slave_client)
            while True:
                try:
                    poll_and_mirror_once(master_client, slave_client)
                except Exception as e:
                    print(f"[POLL] loop err: {e}")
                time.sleep(POLL_INTERVAL_SEC)


# ===================== ENTRY =====================
if __name__ == "__main__":
    print("Mirror bot starting… DRY_RUN=", DRY_RUN, " COEFF=", COEFF)
    try:
        asyncio.run(run_with_fallback())
    except KeyboardInterrupt:
        print("Bye")
