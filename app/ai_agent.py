import inspect
import json
import os
from datetime import datetime

from openai import OpenAI

from app import ai_tools

from pathlib import Path

GLOSARIUM = (Path(__file__).parent / "knowledge" / "glosarium.md").read_text(encoding="utf-8")

MODEL = os.environ["GROQ_MODEL"]
client = OpenAI(api_key=os.environ["GROQ_API_KEY"],
                base_url="https://api.groq.com/openai/v1")

SYSTEM = """Kamu asisten data hidrologi SIHKA BBWS Citanduy.
- Jawab dalam Bahasa Indonesia, singkat.
- Hanya pakai data dari tool. Kalau tidak ada data, katakan tidak ada. Jangan menebak angka.
- Cari pos dulu dengan cari_pos untuk mendapat pos_id.
- Satuan: TMA dalam cm, hujan dalam mm.
- TMA 0 cm setelah sebelumnya ada nilai positif kemungkinan sensor bermasalah; sebutkan itu.
- Hujan hari ini belum total sehari; sebutkan jam data terakhirnya.
- Untuk pos terputus, sebutkan ambang jamnya dan jam sekarang. Tampilkan hanya nama pos dan waktu data terakhir.
- Jangan tampilkan pos_id atau kode tipe ke pengguna.
- Untuk pertanyaan istilah atau jenis pos, jawab hanya dari bagian "Pengetahuan resmi" di bawah. Kalau tidak tercantum di sana, katakan tidak ada.
- Untuk pos terputus pakai jam=3 kecuali pengguna menyebut ambang lain, dan selalu sebutkan ambangnya.
- Status TMA mengikuti ambang pos (Normal, Siaga Hijau, Kuning, Merah). Siaga hujan jika hujan lebih dari 5 mm dalam 60 menit. Untuk pertanyaan kondisi pos, pakai kondisi_pos.
Sekarang: {now} WIB."""


def _tgl(s):
    return datetime.strptime(s, "%Y-%m-%d").date() if s else None


# Pembungkus bertipe supaya skema tool jelas bagi LLM
def cari_pos(keyword: str) -> list:
    """Cari pos berdasarkan nama. Hasilnya berisi pos_id."""
    return [{k: v for k, v in p.items() if k != "latest_up"}
            for p in ai_tools.cari_pos(keyword)]

def tma_terakhir(pos_id: int) -> dict:
    """TMA (cm) valid terakhir hari ini untuk satu pos."""
    return ai_tools.tma_terakhir(pos_id)

def hujan_hari_ini(pos_id: int, tanggal: str = "") -> dict:
    """Total hujan (mm) satu pos pada tanggal YYYY-MM-DD; kosong = hari ini sampai sekarang."""
    return ai_tools.hujan_hari_ini(pos_id, _tgl(tanggal))

def hujan_tertinggi(tanggal: str = "", top: int = 5) -> list:
    """Pos dengan hujan tertinggi pada tanggal YYYY-MM-DD; kosong = hari ini."""
    return ai_tools.hujan_tertinggi(_tgl(tanggal), top)

def hujan_harian(pos_id: int, hari: int = 7) -> dict:
    """Hujan (mm) per tanggal untuk satu pos, beberapa hari terakhir. Tanggal tanpa data tidak muncul."""
    return ai_tools.hujan_harian(pos_id, hari)

def pos_terputus(jam: int = 3) -> list:
    """Pos yang belum mengirim data lebih dari sekian jam (default 3)."""
    return ai_tools.pos_terputus(jam)

def kondisi_pos(pos_id: int) -> dict:
    """Kondisi terkini satu pos: TMA (cm) dan status siaga, hujan 60 menit terakhir (mm), waktu data terakhir."""
    return ai_tools.kondisi_pos(pos_id)


TOOLS = [cari_pos, tma_terakhir, hujan_hari_ini, hujan_tertinggi, hujan_harian, pos_terputus]


def _schema(fn):
    tipe = {int: "integer", str: "string"}
    sig = inspect.signature(fn)
    props = {}
    for n, p in sig.parameters.items():
        t = tipe.get(p.annotation, "string")
        opsional = p.default is not inspect.Parameter.empty
        props[n] = {"type": [t, "null"] if opsional else t}
    req = [n for n, p in sig.parameters.items() if p.default is inspect.Parameter.empty]
    return {"type": "function", "function": {
        "name": fn.__name__, "description": fn.__doc__,
        "parameters": {"type": "object", "properties": props, "required": req}}}


SCHEMAS = [_schema(f) for f in TOOLS]
REGISTRY = {f.__name__: f for f in TOOLS}


def ask(pertanyaan: str, maks_langkah: int = 6) -> str:
    system = SYSTEM.format(now=datetime.now().strftime("%Y-%m-%d %H:%M")) \
        + "\n\nPengetahuan resmi:\n" + GLOSARIUM
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": pertanyaan},
    ]
    for _ in range(maks_langkah):
        r = client.chat.completions.create(
            model=MODEL, messages=messages, tools=SCHEMAS, temperature=0)
        msg = r.choices[0].message
        if not msg.tool_calls:
            return msg.content
        messages.append({"role": "assistant", "content": msg.content or "", "tool_calls": [
            {"id": tc.id, "type": "function",
             "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
            for tc in msg.tool_calls]})
        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments or "{}")
                args = {k: v for k, v in args.items() if v is not None}
                hasil = REGISTRY[tc.function.name](**args)
            except Exception as e:
                hasil = {"error": str(e)}
            messages.append({"role": "tool", "tool_call_id": tc.id,
                             "content": json.dumps(hasil, default=str)})
    return "Gagal: terlalu banyak langkah tool."