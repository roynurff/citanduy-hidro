from datetime import datetime, timedelta
from app.models import db  # sesuaikan dengan objek database Peewee kamu


def cari_pos(keyword: str, limit: int = 5):
    """Cari pos berdasarkan nama (tidak peka huruf besar/kecil)."""
    cur = db.execute_sql(
        "SELECT id, nama, tipe, sungai, kecamatan, kabupaten, latest_up "
        "FROM pos WHERE nama ILIKE %s ORDER BY nama LIMIT %s",
        (f"%{keyword}%", limit),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def pos_terputus(jam: int = 6):
    """Pos yang belum kirim data lebih dari `jam` jam terakhir."""
    batas = datetime.now() - timedelta(hours=jam)
    cur = db.execute_sql(
        "SELECT id, nama, tipe, latest_up FROM pos "
        "WHERE latest_up IS NULL OR latest_up < %s ORDER BY latest_up NULLS FIRST",
        (batas,),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


import json


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def parse_raw(raw):
    """raw JSON rdaily -> list pembacaan terurut waktu."""
    try:
        items = json.loads(raw)
    except (TypeError, ValueError):
        return []
    out = []
    for it in items:
        try:
            t = datetime.fromisoformat(it["sampling"])
        except (KeyError, ValueError, TypeError):
            continue
        out.append({"t": t, "rain": _f(it.get("rain")), "wlevel": _f(it.get("wlevel"))})
    out.sort(key=lambda x: x["t"])
    return out


def tma_terakhir(pos_id: int):
    """TMA valid terakhir hari ini. wlevel <= 0 dianggap sensor mati."""
    cur = db.execute_sql(
        "SELECT raw FROM rdaily WHERE pos_id = %s AND sampling = CURRENT_DATE",
        (pos_id,),
    )
    data = []
    for (raw,) in cur.fetchall():
        data += parse_raw(raw)
    data.sort(key=lambda x: x["t"])
    valid = [d for d in data if d["wlevel"] is not None and d["wlevel"] > 0]
    return {
        "pos_id": pos_id,
        "data_masuk_terakhir": data[-1]["t"].isoformat() if data else None,
        "tma_valid_terakhir": valid[-1]["wlevel"] if valid else None,
        "waktu_tma_valid": valid[-1]["t"].isoformat() if valid else None,
    }