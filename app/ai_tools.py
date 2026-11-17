from datetime import datetime, timedelta
from app.models import Pos

db = Pos._meta.database

def cari_pos(keyword: str, limit: int = 5):
    """Cari pos berdasarkan nama (tidak peka huruf besar/kecil)."""
    cur = db.execute_sql(
        "SELECT id, nama, tipe, sungai, kecamatan, kabupaten, latest_up "
        "FROM pos WHERE nama ILIKE %s ORDER BY nama LIMIT %s",
        (f"%{keyword}%", limit),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _pembacaan_terakhir_per_pos(hari: int = 7):
    """{pos_id: waktu pembacaan terakhir} dari rdaily, `hari` hari ke belakang."""
    cur = db.execute_sql(
        "SELECT pos_id, raw FROM rdaily "
        "WHERE pos_id IS NOT NULL AND sampling BETWEEN CURRENT_DATE - %s AND CURRENT_DATE",
        (hari,),
    )
    last = {}
    for pos_id, raw in cur.fetchall():
        data = parse_raw(raw)
        if data and (pos_id not in last or data[-1]["t"] > last[pos_id]):
            last[pos_id] = data[-1]["t"]
    return last


def pos_terputus(jam: int = 3):
    """Pos telemetri (punya data 7 hari terakhir) yang belum kirim data > `jam` jam."""
    last = _pembacaan_terakhir_per_pos()
    batas = datetime.now() - timedelta(hours=jam)
    cur = db.execute_sql(
        "SELECT id, nama, tipe FROM pos WHERE id = ANY(%s) ORDER BY nama",
        (list(last),),
    )
    hasil = []
    for id_, nama, tipe in cur.fetchall():
        if last[id_] < batas:
            hasil.append({"id": id_, "nama": nama, "tipe": tipe,
                          "data_terakhir": last[id_].isoformat()})
    return hasil


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
    terakhir = data[-1] if data else None
    return {
        "pos_id": pos_id,
        "satuan": "cm",
        "data_masuk_terakhir": terakhir["t"].isoformat() if terakhir else None,
        "wlevel_pembacaan_terakhir": terakhir["wlevel"] if terakhir else None,
        "tma_valid_terakhir": valid[-1]["wlevel"] if valid else None,
        "waktu_tma_valid": valid[-1]["t"].isoformat() if valid else None,
    }


RAIN_MAX = 1000  # mm; batas wajar, buang nilai sampah


def _hujan_dari_raw(raw, source):
    vals = [
        d["rain"] for d in parse_raw(raw)
        if d["rain"] is not None and 0 <= d["rain"] < RAIN_MAX
    ]
    if not vals:
        return 0.0
    if source in ("SA", "SB"):          # per interval
        return sum(vals)
    # kumulatif (reset ~07:00): jumlahkan kenaikan positif saja
    return sum(b - a for a, b in zip(vals, vals[1:]) if b > a)


def hujan_hari_ini(pos_id: int, tanggal=None):
    """Total hujan (mm) pada tanggal rdaily (default hari ini, 00:00 sampai sekarang)."""
    cur = db.execute_sql(
        "SELECT source, raw FROM rdaily "
        "WHERE pos_id = %s AND sampling = COALESCE(%s, CURRENT_DATE)",
        (pos_id, tanggal),
    )
    # max, bukan sum: satu pos satu penakar, jangan dobel kalau ada 2 baris/vendor
    total = max((_hujan_dari_raw(raw, src) for src, raw in cur.fetchall()), default=0.0)
    return {"pos_id": pos_id, "hujan_mm": round(total, 1)}